use std::cell::RefCell;
use std::rc::Rc;

use futures::{Sink, AsyncSink, StartSend, Poll, Async};
use futures::task::{self, Task};
use ordermap::OrderSet;

use id_task::IdTask;

/// A *m*ulti *p*roducer *s*ink (`MPS`). This is a cloneable handle to a single
/// sink of type `S`, and each handle can be used to write to the inner sink.
///
/// Errors are simply propagated to the handle that triggered it, but they are
/// not stored. This means that other handles might still try to write to the
/// sink after it errored. Make sure to use a sink that can deal with this.
///
/// Each of the handles must invoke `close` before being dropped. The inner sink
/// is closed when each of the handles has `close`d.
pub struct MPS<S> {
    shared: Rc<RefCell<Shared<S>>>,
    did_close: bool,
    // https://github.com/alexcrichton/futures-rs/issues/670
    // id may never be 0, 0 signals that nothing is blocking
    id: usize,
}

impl<S> MPS<S> {
    /// Create a new MPS, wrapping the given sink.
    pub fn new(sink: S) -> MPS<S> {
        MPS {
            shared: Rc::new(RefCell::new(Shared::new(sink))),
            did_close: false,
            id: 1,
        }
    }
}

/// Performs minimal cleanup to allow for correct closing behaviour
impl<S> Drop for MPS<S> {
    fn drop(&mut self) {
        if self.did_close {
            self.shared.borrow_mut().decrement_close_count();
        }
    }
}

impl<S> Clone for MPS<S> {
    /// Returns a new handle to the same underlying sink.
    fn clone(&self) -> MPS<S> {
        MPS {
            shared: self.shared.clone(),
            // New handle has not closed yet, so always set this to false.
            did_close: false,
            id: self.shared.borrow_mut().next_id(),
        }
    }
}

impl<S: Sink> Sink for MPS<S> {
    type SinkItem = S::SinkItem;
    type SinkError = S::SinkError;

    /// Start sending to the inner sink. When multiple handles block on this, the
    /// tasks are notified in a fifo pattern (or they will be once the ordermap
    /// crate fixes its `OrderSet::remove` implementation).
    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        self.shared.borrow_mut().do_start_send(item, self.id)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.shared.borrow_mut().do_poll_complete(self.id)
    }

    /// This only delegates to the `close` method of the inner sink if all other
    /// active handles have already called close. Else, it simply flushes the
    /// underlying sink, but does not close it.
    ///
    /// Calling `clone` after calling `close` leads to ambiguity whether the
    /// inner sink has actually closed yet. It's still safe, but not advisable.
    fn close(&mut self) -> Poll<(), Self::SinkError> {
        let mut shared = self.shared.borrow_mut();

        if !self.did_close {
            shared.close_count += 1;
            self.did_close = true;
        }

        shared.do_close(Rc::strong_count(&self.shared), self.id)
    }
}

struct Shared<S> {
    inner: S,
    // Counter to provide unique ids to each handle (because we can't check
    // `Task`s for equality directly), see https://github.com/alexcrichton/futures-rs/issues/670
    id_counter: usize,
    // The number of the currently active handles that have called `close`.
    // When a handle goes out of scope, it decrements this counter if it has
    // previously incremented it (by calling `do_close`).
    // A call to `do_close` only delegates to the inner sink if the number of
    // active handles equals the close count.
    close_count: usize,
    // queue of tasks that tried to send/flush while the inner sink was blocking
    tasks: OrderSet<IdTask>,
    // The id of the handle on whose call the inner sink is currently blocking.
    // Is set to zero when inner sink is not currently blocking.
    current: usize,
}

impl<S> Shared<S> {
    fn new(sink: S) -> Shared<S> {
        Shared {
            inner: sink,
            id_counter: 2,
            close_count: 0,
            tasks: OrderSet::new(),
            current: 0,
        }
    }

    fn decrement_close_count(&mut self) {
        self.close_count -= 1;
    }

    fn next_id(&mut self) -> usize {
        self.id_counter += 1;
        return self.id_counter - 1;
    }
}

impl<S: Sink> Shared<S> {
    fn do_start_send(&mut self,
                     item: S::SinkItem,
                     id: usize)
                     -> StartSend<S::SinkItem, S::SinkError> {
        // Either the inner sink is not blocking at all, or it is blocking for
        // the handle which called this method. In both scenarios we can try to
        // make progress.
        if self.current == 0 || id == self.current {
            match self.inner.start_send(item) {
                Ok(AsyncSink::Ready) => {
                    // Done with this item. If further tasks are waiting, notify
                    // the next one.
                    self.current = 0;

                    let front;
                    match self.tasks.get_index(0) {
                        None => {
                            return Ok(AsyncSink::Ready);
                        }
                        Some(id_t) => {
                            front = id_t.clone();
                        }
                    }

                    front.task.notify();
                    self.tasks.remove(&front);
                    return Ok(AsyncSink::Ready);
                }

                Ok(AsyncSink::NotReady(it)) => {
                    // Inner sink is now (and may have already been) blocking on this handle.
                    self.current = id;
                    Ok(AsyncSink::NotReady(it))
                }

                Err(e) => {
                    self.current = 0;
                    Err(e)
                }
            }
        } else {
            // Can not make progress, enqueue the task.
            self.tasks.insert(IdTask::new(task::current(), id));
            Ok(AsyncSink::NotReady(item))
        }
    }

    fn do_poll_complete(&mut self, id: usize) -> Poll<(), S::SinkError> {
        // Either the inner sink is not blocking at all, or it is blocking for
        // the handle which called this method. In both scenarios we can try to
        // make progress.
        if self.current == 0 || id == self.current {
            match self.inner.poll_complete() {
                Ok(Async::Ready(_)) => {
                    // Done flushing. If further tasks are waiting, notify
                    // the next one.
                    self.current = 0;

                    let front;
                    match self.tasks.get_index(0) {
                        None => {
                            return Ok(Async::Ready(()));
                        }
                        Some(id_t) => {
                            front = id_t.clone();
                        }
                    }

                    front.task.notify();
                    self.tasks.remove(&front);
                    return Ok(Async::Ready(()));
                }

                Ok(Async::NotReady) => {
                    // Inner sink is now (and may have already been) blocking on this handle.
                    self.current = id;
                    Ok(Async::NotReady)
                }

                Err(e) => {
                    self.current = 0;
                    Err(e)
                }
            }
        } else {
            // Can not make progress, enqueue the task.
            self.tasks.insert(IdTask::new(task::current(), id));
            Ok(Async::NotReady)
        }
    }

    fn do_close(&mut self, ref_count: usize, id: usize) -> Poll<(), S::SinkError> {
        // Either the inner sink is not blocking at all, or it is blocking for
        // the handle which called this method. In both scenarios we can try to
        // make progress.
        if self.current == 0 || id == self.current {
            // close the inner sink
            if (ref_count == self.close_count || ref_count == 1) && self.tasks.is_empty() {
                match self.inner.close() {
                    Ok(Async::Ready(_)) => {
                        // Done closing.
                        self.current = 0;

                        self.tasks.pop();
                        return Ok(Async::Ready(()));
                    }

                    Ok(Async::NotReady) => {
                        // Inner sink is now (and may have already been) blocking on this handle.
                        self.current = id;
                        Ok(Async::NotReady)
                    }

                    Err(e) => {
                        self.current = 0;
                        Err(e)
                    }
                }

                // only flush
            } else {
                match self.inner.poll_complete() {
                    Ok(Async::Ready(_)) => {
                        // Done closing. If further tasks are waiting, notify
                        // the next one.
                        self.current = 0;

                        let front;
                        match self.tasks.get_index(0) {
                            None => {
                                return Ok(Async::Ready(()));
                            }
                            Some(id_t) => {
                                front = id_t.clone();
                            }
                        }

                        front.task.notify();
                        self.tasks.remove(&front);
                        return Ok(Async::Ready(()));
                    }

                    Ok(Async::NotReady) => {
                        // Inner sink is now (and may have already been) blocking on this handle.
                        self.current = id;
                        Ok(Async::NotReady)
                    }

                    Err(e) => {
                        self.current = 0;
                        Err(e)
                    }
                }
            }
        } else {
            // Can not make progress, enqueue the task.
            self.tasks.insert(IdTask::new(task::current(), id));
            Ok(Async::NotReady)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::{Future, Stream};
    use futures::stream::iter_ok;

    use quickcheck::{QuickCheck, StdGen};
    use rand;
    use void::Void;
    use atm_async_utils::test_channel::*;
    use atm_async_utils::test_sink::*;

    #[test]
    fn test_success() {
        let rng = StdGen::new(rand::thread_rng(), 50);
        let mut quickcheck = QuickCheck::new().gen(rng).tests(1000);
        quickcheck.quickcheck(success as fn(usize) -> bool);
    }

    fn success(buf_size: usize) -> bool {
        let (sender, receiver) = test_channel::<u8, Void, Void>(buf_size + 1);

        let s1 = MPS::new(sender);
        let s2 = s1.clone();
        let s3 = s1.clone();
        let s4 = s1.clone();

        let send_all1 = s1.send_all(iter_ok::<_, Void>(0..10));
        let send_all2 = s2.send_all(iter_ok::<_, Void>(10..20));
        let send_all3 = s3.send_all(iter_ok::<_, Void>(20..30));
        let send_all4 = s4.send_all(iter_ok::<_, Void>(30..40));
        let sending = send_all1.join4(send_all2, send_all3, send_all4);

        let (mut received, _) = receiver.collect().join(sending).wait().unwrap();
        received.sort();

        return received == (0..40).collect::<Vec<u8>>();
    }

    #[test]
    fn test_error() {
        let (sender, _) = test_channel::<u8, u8, Void>(8);
        let s1 = MPS::new(TestSink::new(sender,
                                        vec![SendOp::Delegate, SendOp::Err(13), SendOp::Delegate],
                                        vec![]));
        let s2 = s1.clone();

        let s1 = s1.send(42).wait().unwrap();
        assert_eq!(s2.send(42).wait().err().unwrap(), 13);
        assert!(s1.send(42).wait().is_ok());
    }

    struct CloseTester(usize);

    impl Sink for CloseTester {
        type SinkItem = ();
        type SinkError = ();

        fn start_send(&mut self, _: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
            Ok(AsyncSink::Ready)
        }

        fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
            Ok(Async::Ready(()))
        }

        fn close(&mut self) -> Poll<(), Self::SinkError> {
            self.0 += 1;
            if self.0 > 1 {
                Err(())
            } else {
                Ok(Async::Ready(()))
            }
        }
    }

    #[test]
    fn test_close() {
        let mut s1 = MPS::new(CloseTester(0));
        let mut s2 = s1.clone();

        assert_eq!(s1.close(), Ok(Async::Ready(())));

        {
            let s3 = s1.clone();
            let _ = s3.flush();
        }

        assert_eq!(s2.close(), Ok(Async::Ready(())));
    }
}
