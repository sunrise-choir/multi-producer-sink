//! This crate provides a cloneable wrapper around sinks that allow multiple,
//! independent tasks to write to the same underlying sink.
#![deny(missing_docs)]

extern crate futures;
extern crate ordermap;

#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
extern crate rand;
#[cfg(test)]
extern crate void;
#[cfg(test)]
extern crate atm_async_utils;

use std::cell::RefCell;
use std::rc::Rc;
use std::hash::{Hash, Hasher};

use futures::{Future, Sink, AsyncSink, StartSend, Poll, Async};
use futures::task::{self, Task};
use futures::unsync::oneshot::{Receiver, Sender, Canceled, channel};
use ordermap::OrderSet;

/// Create a new MPS, wrapping the given sink. Also returns a future that
/// emits the wrapped sink once the last `MPS` handle has been closed.
pub fn mps<S>(sink: S) -> (MPS<S>, Closed<S>) {
    let (sender, receiver) = channel();

    (MPS {
         shared: Rc::new(RefCell::new(Shared::new(sink, sender))),
         did_close: false,
         id: 1,
     },
     Closed(receiver))
}

/// A future that emits the wrapped sink once the last handle has been closed,
/// or once an error has been encountered.
///
/// This has the dual purpose of returning ownership of the inner sink, and
/// signaling that the inner sink has been fully closed (unless it errored).
pub struct Closed<S>(Receiver<S>);

impl<S> Future for Closed<S> {
    type Item = S;
    /// This can only be emitted if not error occured but all `MPS`s are dropped
    /// without closing them first.
    type Error = Canceled;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
    }
}

/// A multi producer sink (`MPS`). This is a cloneable handle to a single
/// sink of type `S`, and each handle can be used to write to the inner sink.
///
/// An error is propagated as a `Err(Some(err))` to the handle that triggered it,
/// and all other handles emit an `Err(None)`. All further polling will always
/// yield `Err(None)`.
///
/// Unless an error occured, each of the handles must invoke `close` before
/// being dropped. The inner sink is closed when each of the handles has `close`d.
pub struct MPS<S> {
    shared: Rc<RefCell<Shared<S>>>,
    did_close: bool,
    // https://github.com/alexcrichton/futures-rs/issues/670
    // id may never be 0, 0 signals that nothing is blocking
    id: usize,
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
    type SinkError = Option<S::SinkError>;

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
        let mut close_count = shared.close_count;

        if !self.did_close {
            shared.increment_close_count();
            close_count += 1;
            self.did_close = true;
        }

        shared.do_close(self.id, Rc::strong_count(&self.shared) == close_count)
    }
}

struct Shared<S> {
    inner: Option<S>,
    sender: Option<Sender<S>>,
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
    errored: bool,
}

impl<S> Shared<S> {
    fn new(sink: S, sender: Sender<S>) -> Shared<S> {
        Shared {
            inner: Some(sink),
            sender: Some(sender),
            id_counter: 2,
            close_count: 0,
            tasks: OrderSet::new(),
            current: 0,
            errored: false,
        }
    }

    fn increment_close_count(&mut self) {
        self.close_count += 1;
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
                     -> StartSend<S::SinkItem, Option<S::SinkError>> {
        if self.errored {
            return Err(None);
        }

        let mut inner = self.inner.take().unwrap();

        // Either the inner sink is not blocking at all, or it is blocking for
        // the handle which called this method. In both scenarios we can try to
        // make progress.
        if self.current == 0 || id == self.current {
            match inner.start_send(item) {
                Ok(AsyncSink::Ready) => self.notify_next_start_send(inner),

                Ok(AsyncSink::NotReady(it)) => {
                    // Inner sink is now (and may have already been) blocking on this handle.
                    self.current = id;
                    self.inner = Some(inner);
                    Ok(AsyncSink::NotReady(it))
                }

                Err(e) => self.error_start_send(inner, e),
            }
        } else {
            // Can not make progress, enqueue the task.
            self.tasks.insert(IdTask::new(task::current(), id));
            self.inner = Some(inner);
            Ok(AsyncSink::NotReady(item))
        }
    }

    fn do_poll_complete(&mut self, id: usize) -> Poll<(), Option<S::SinkError>> {
        if self.errored {
            return Err(None);
        }

        let mut inner = self.inner.take().unwrap();

        // Either the inner sink is not blocking at all, or it is blocking for
        // the handle which called this method. In both scenarios we can try to
        // make progress.
        if self.current == 0 || id == self.current {
            match inner.poll_complete() {
                Ok(Async::Ready(_)) => self.notify_next(inner),

                Ok(Async::NotReady) => {
                    // Inner sink is now (and may have already been) blocking on this handle.
                    self.current = id;
                    self.inner = Some(inner);
                    Ok(Async::NotReady)
                }

                Err(e) => self.error(inner, e),
            }
        } else {
            // Can not make progress, enqueue the task.
            self.tasks.insert(IdTask::new(task::current(), id));
            self.inner = Some(inner);
            Ok(Async::NotReady)
        }
    }

    fn do_close(&mut self, id: usize, really_close: bool) -> Poll<(), Option<S::SinkError>> {
        if self.errored {
            return Err(None);
        }

        let mut inner = self.inner.take().unwrap();

        // Either the inner sink is not blocking at all, or it is blocking for
        // the handle which called this method. In both scenarios we can try to
        // make progress.
        if self.current == 0 || id == self.current {
            // close the inner sink
            if (really_close) && self.tasks.is_empty() {
                match inner.close() {
                    Ok(Async::Ready(_)) => {
                        // Done closing.
                        self.current = 0;

                        self.tasks.pop();

                        let _ = self.sender.take().unwrap().send(inner);
                        return Ok(Async::Ready(()));
                    }

                    Ok(Async::NotReady) => {
                        // Inner sink is now (and may have already been) blocking on this handle.
                        self.current = id;
                        self.inner = Some(inner);
                        Ok(Async::NotReady)
                    }

                    Err(e) => self.error(inner, e),
                }

                // only flush
            } else {
                match inner.poll_complete() {
                    Ok(Async::Ready(_)) => self.notify_next(inner),

                    Ok(Async::NotReady) => {
                        // Inner sink is now (and may have already been) blocking on this handle.
                        self.current = id;
                        self.inner = Some(inner);
                        Ok(Async::NotReady)
                    }

                    Err(e) => self.error(inner, e),
                }
            }
        } else {
            // Can not make progress, enqueue the task.
            self.tasks.insert(IdTask::new(task::current(), id));
            self.inner = Some(inner);
            Ok(Async::NotReady)
        }
    }

    fn notify(&mut self) {
        let front;
        match self.tasks.get_index(0) {
            None => {
                return;
            }
            Some(id_t) => {
                front = id_t.clone();
            }
        }
        front.task.notify();
        self.tasks.remove(&front);
    }

    fn notify_next_start_send(&mut self, inner: S) -> StartSend<S::SinkItem, Option<S::SinkError>> {
        self.current = 0;
        self.inner = Some(inner);
        self.notify();
        return Ok(AsyncSink::Ready);
    }

    fn error_start_send(&mut self,
                        inner: S,
                        err: S::SinkError)
                        -> StartSend<S::SinkItem, Option<S::SinkError>> {
        self.errored = true;
        let _ = self.notify_next_start_send(inner); // always `Ok(AsyncSink::Ready)`
        Err(Some(err))
    }

    fn notify_next(&mut self, inner: S) -> Poll<(), Option<S::SinkError>> {
        self.current = 0;
        self.inner = Some(inner);
        self.notify();
        return Ok(Async::Ready(()));
    }

    fn error(&mut self, inner: S, err: S::SinkError) -> Poll<(), Option<S::SinkError>> {
        self.errored = true;
        let _ = self.notify_next(inner); // always `Ok(Async::Ready(()))`
        Err(Some(err))
    }
}

#[derive(Clone)]
struct IdTask {
    pub task: Task,
    pub id: usize,
}

impl IdTask {
    fn new(task: Task, id: usize) -> IdTask {
        IdTask { task, id }
    }
}

impl Hash for IdTask {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

impl PartialEq for IdTask {
    fn eq(&self, rhs: &IdTask) -> bool {
        self.id == rhs.id
    }
}

impl Eq for IdTask {}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::{Future, Stream, AsyncSink, Async};
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
        let (sender, receiver) = test_channel::<u8, Canceled, Option<Canceled>>(buf_size + 1);

        let (s1, closed) = mps(sender);
        let s2 = s1.clone();
        let s3 = s1.clone();
        let s4 = s1.clone();

        let send_all1 = s1.send_all(iter_ok::<_, Canceled>(0..10));
        let send_all2 = s2.send_all(iter_ok::<_, Canceled>(10..20));
        let send_all3 = s3.send_all(iter_ok::<_, Canceled>(20..30));
        let send_all4 = s4.send_all(iter_ok::<_, Canceled>(30..40));
        let sending = send_all1.join4(send_all2, send_all3, send_all4);

        let (mut received, _, _) = receiver
            .collect()
            .join3(sending, closed.map_err(|err| Some(err)))
            .wait()
            .unwrap();
        received.sort();

        return received == (0..40).collect::<Vec<u8>>();
    }

    #[test]
    fn test_error() {
        let (sender, _) = test_channel::<u8, u8, Void>(8);
        let (s1, closed) =
            mps(TestSink::new(sender,
                              vec![SendOp::Delegate, SendOp::Err(13), SendOp::Delegate],
                              vec![]));
        let s2 = s1.clone();
        let s3 = s1.clone();
        let s4 = s1.clone();

        let send_all1 = s1.send_all(iter_ok::<_, Option<u8>>(0..10));
        let send_all2 = s2.send_all(iter_ok::<_, Option<u8>>(10..20));
        let send_all3 = s3.send_all(iter_ok::<_, Option<u8>>(20..30));
        let send_all4 = s4.send_all(iter_ok::<_, Option<u8>>(30..40));
        let sending = send_all1.join4(send_all2, send_all3, send_all4);

        assert_eq!(closed
                       .map_err(|_| None)
                       .join(sending)
                       .wait()
                       .err()
                       .unwrap(),
                   Some(13));
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
        let (mut s1, closed) = mps(CloseTester(0));
        let mut s2 = s1.clone();

        assert_eq!(s1.close(), Ok(Async::Ready(())));

        {
            let s3 = s1.clone();
            let _ = s3.flush();
        }

        assert_eq!(s2.close(), Ok(Async::Ready(())));
        let _ = closed.wait(); // does not hang forever
    }
}
