use std::cell::RefCell;
use std::rc::Rc;
use std::hash::{Hash, Hasher};

use futures::{Future, Sink, AsyncSink, StartSend, Poll, Async};
use futures::task::{self, Task};
use futures::unsync::oneshot::{Receiver, Sender, Canceled, channel};
use ordermap::OrderSet;

/// A future that emits the wrapped sink once the last handle has been closed.
///
/// This has the dual purpose of returning ownership of the inner sink, and
/// signaling that the sink has been fully closed.
pub struct Closed<S>(Receiver<S>);

impl<S> Future for Closed<S> {
    type Item = S;
    /// This can only be emitted if `MPS`s are dropped without closing them
    /// first. As long as you uphold the `Sink` contract of always closing
    /// before dropping, this will never be emitted.
    type Error = Canceled;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
    }
}

/// A multi producer sink (`MPS`). This is a cloneable handle to a single
/// sink of type `S`, and each handle can be used to write to the inner sink.
///
/// Errors are simply propagated to the handle that triggered it, but they are
/// not stored. This means that other handles might still try to write to the
/// sink after it errored. Make sure to use a sink that can deal with this.
///
/// Each of the handles must invoke `close` before being dropped. The inner sink
/// is closed when each of the handles has `close`d.
///
/// Internally, this uses reference counting to keep track of the clones.
pub struct MPS<S> {
    shared: Rc<RefCell<Shared<S>>>,
    did_close: bool,
    // https://github.com/alexcrichton/futures-rs/issues/670
    // id may never be 0, 0 signals that nothing is blocking
    id: usize,
}

impl<S> MPS<S> {
    /// Create a new MPS, wrapping the given sink. Also returns a future that
    /// emits the wrapped sink once the last `MPS` handle has been closed.
    pub fn new(sink: S) -> (MPS<S>, Closed<S>) {
        let (sender, receiver) = channel();

        (MPS {
             shared: Rc::new(RefCell::new(Shared::new(sink, sender))),
             did_close: false,
             id: 1,
         },
         Closed(receiver))
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
                     -> StartSend<S::SinkItem, S::SinkError> {
        let mut inner = self.inner.take().unwrap();

        // Either the inner sink is not blocking at all, or it is blocking for
        // the handle which called this method. In both scenarios we can try to
        // make progress.
        if self.current == 0 || id == self.current {
            match inner.start_send(item) {
                Ok(AsyncSink::Ready) => {
                    // Done with this item. If further tasks are waiting, notify
                    // the next one.
                    self.current = 0;

                    let front;
                    match self.tasks.get_index(0) {
                        None => {
                            self.inner = Some(inner);
                            return Ok(AsyncSink::Ready);
                        }
                        Some(id_t) => {
                            front = id_t.clone();
                        }
                    }

                    front.task.notify();
                    self.tasks.remove(&front);
                    self.inner = Some(inner);
                    return Ok(AsyncSink::Ready);
                }

                Ok(AsyncSink::NotReady(it)) => {
                    // Inner sink is now (and may have already been) blocking on this handle.
                    self.current = id;
                    self.inner = Some(inner);
                    Ok(AsyncSink::NotReady(it))
                }

                Err(e) => {
                    self.current = 0;
                    self.inner = Some(inner);
                    Err(e)
                }
            }
        } else {
            // Can not make progress, enqueue the task.
            self.tasks.insert(IdTask::new(task::current(), id));
            self.inner = Some(inner);
            Ok(AsyncSink::NotReady(item))
        }
    }

    fn do_poll_complete(&mut self, id: usize) -> Poll<(), S::SinkError> {
        let mut inner = self.inner.take().unwrap();

        // Either the inner sink is not blocking at all, or it is blocking for
        // the handle which called this method. In both scenarios we can try to
        // make progress.
        if self.current == 0 || id == self.current {
            match inner.poll_complete() {
                Ok(Async::Ready(_)) => {
                    // Done flushing. If further tasks are waiting, notify
                    // the next one.
                    self.current = 0;

                    let front;
                    match self.tasks.get_index(0) {
                        None => {
                            self.inner = Some(inner);
                            return Ok(Async::Ready(()));
                        }
                        Some(id_t) => {
                            front = id_t.clone();
                        }
                    }

                    front.task.notify();
                    self.tasks.remove(&front);
                    self.inner = Some(inner);
                    return Ok(Async::Ready(()));
                }

                Ok(Async::NotReady) => {
                    // Inner sink is now (and may have already been) blocking on this handle.
                    self.current = id;
                    self.inner = Some(inner);
                    Ok(Async::NotReady)
                }

                Err(e) => {
                    self.current = 0;
                    self.inner = Some(inner);
                    Err(e)
                }
            }
        } else {
            // Can not make progress, enqueue the task.
            self.tasks.insert(IdTask::new(task::current(), id));
            self.inner = Some(inner);
            Ok(Async::NotReady)
        }
    }

    fn do_close(&mut self, id: usize, really_close: bool) -> Poll<(), S::SinkError> {
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

                    Err(e) => {
                        self.current = 0;
                        self.inner = Some(inner);
                        Err(e)
                    }
                }

                // only flush
            } else {
                match inner.poll_complete() {
                    Ok(Async::Ready(_)) => {
                        // Done closing. If further tasks are waiting, notify
                        // the next one.
                        self.current = 0;

                        let front;
                        match self.tasks.get_index(0) {
                            None => {
                                self.inner = Some(inner);
                                return Ok(Async::Ready(()));
                            }
                            Some(id_t) => {
                                front = id_t.clone();
                            }
                        }

                        front.task.notify();
                        self.tasks.remove(&front);
                        self.inner = Some(inner);
                        return Ok(Async::Ready(()));
                    }

                    Ok(Async::NotReady) => {
                        // Inner sink is now (and may have already been) blocking on this handle.
                        self.current = id;
                        self.inner = Some(inner);
                        Ok(Async::NotReady)
                    }

                    Err(e) => {
                        self.current = 0;
                        self.inner = Some(inner);
                        Err(e)
                    }
                }
            }
        } else {
            // Can not make progress, enqueue the task.
            self.tasks.insert(IdTask::new(task::current(), id));
            self.inner = Some(inner);
            Ok(Async::NotReady)
        }
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
        let (sender, receiver) = test_channel::<u8, Canceled, Canceled>(buf_size + 1);

        let (s1, closed) = MPS::new(sender);
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
            .join3(sending, closed)
            .wait()
            .unwrap();
        received.sort();

        return received == (0..40).collect::<Vec<u8>>();
    }

    #[test]
    fn test_error() {
        let (sender, _) = test_channel::<u8, u8, Void>(8);
        let (s1, _) =
            MPS::new(TestSink::new(sender,
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
        let (mut s1, closed) = MPS::new(CloseTester(0));
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


// use std::cell::RefCell;
// use std::rc::Rc;
//
// use futures::{Sink, StartSend, Poll};
//
// use shared::*;
//
// /// A multi producer sink (`MPS`). This is a cloneable handle to a single
// /// sink of type `S`, and each handle can be used to write to the inner sink.
// ///
// /// Errors are simply propagated to the handle that triggered it, but they are
// /// not stored. This means that other handles might still try to write to the
// /// sink after it errored. Make sure to use a sink that can deal with this.
// ///
// /// Each of the handles must invoke `close` before being dropped. The inner sink
// /// is closed when each of the handles has `close`d.
// ///
// /// Internally, this uses reference counting to keep track of the clones.
// /// `OwnerMPS` provides a lifetimes-based alternative with lower runtime
// /// overhead.
// pub struct MPS<S> {
//     shared: Rc<RefCell<Shared<S>>>,
//     did_close: bool,
//     // https://github.com/alexcrichton/futures-rs/issues/670
//     // id may never be 0, 0 signals that nothing is blocking
//     id: usize,
// }
//
// impl<S> MPS<S> {
//     /// Create a new MPS, wrapping the given sink.
//     pub fn new(sink: S) -> MPS<S> {
//         MPS {
//             shared: Rc::new(RefCell::new(Shared::new(sink, 2, 1))),
//             did_close: false,
//             id: 1,
//         }
//     }
// }
//
// /// Performs minimal cleanup to allow for correct closing behaviour
// impl<S> Drop for MPS<S> {
//     fn drop(&mut self) {
//         let mut shared = self.shared.borrow_mut();
//         shared.decrement_ref_count();
//
//         if self.did_close {
//             shared.decrement_close_count();
//         }
//     }
// }
//
// impl<S> Clone for MPS<S> {
//     /// Returns a new handle to the same underlying sink.
//     fn clone(&self) -> MPS<S> {
//         self.shared.borrow_mut().increment_ref_count();
//         MPS {
//             shared: self.shared.clone(),
//             // New handle has not closed yet, so always set this to false.
//             did_close: false,
//             id: self.shared.borrow_mut().next_id(),
//         }
//     }
// }
//
// impl<S: Sink> Sink for MPS<S> {
//     type SinkItem = S::SinkItem;
//     type SinkError = S::SinkError;
//
//     /// Start sending to the inner sink. When multiple handles block on this, the
//     /// tasks are notified in a fifo pattern (or they will be once the ordermap
//     /// crate fixes its `OrderSet::remove` implementation).
//     fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
//         self.shared.borrow_mut().do_start_send(item, self.id)
//     }
//
//     fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
//         self.shared.borrow_mut().do_poll_complete(self.id)
//     }
//
//     /// This only delegates to the `close` method of the inner sink if all other
//     /// active handles have already called close. Else, it simply flushes the
//     /// underlying sink, but does not close it.
//     ///
//     /// Calling `clone` after calling `close` leads to ambiguity whether the
//     /// inner sink has actually closed yet. It's still safe, but not advisable.
//     fn close(&mut self) -> Poll<(), Self::SinkError> {
//         let mut shared = self.shared.borrow_mut();
//
//         if !self.did_close {
//             shared.increment_close_count();
//             self.did_close = true;
//         }
//
//         shared.do_close(self.id)
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     use futures::{Future, Stream, AsyncSink, Async};
//     use futures::stream::iter_ok;
//
//     use quickcheck::{QuickCheck, StdGen};
//     use rand;
//     use void::Void;
//     use atm_async_utils::test_channel::*;
//     use atm_async_utils::test_sink::*;
//
//     #[test]
//     fn test_success() {
//         let rng = StdGen::new(rand::thread_rng(), 50);
//         let mut quickcheck = QuickCheck::new().gen(rng).tests(1000);
//         quickcheck.quickcheck(success as fn(usize) -> bool);
//     }
//
//     fn success(buf_size: usize) -> bool {
//         let (sender, receiver) = test_channel::<u8, Void, Void>(buf_size + 1);
//
//         let s1 = MPS::new(sender);
//         let s2 = s1.clone();
//         let s3 = s1.clone();
//         let s4 = s1.clone();
//
//         let send_all1 = s1.send_all(iter_ok::<_, Void>(0..10));
//         let send_all2 = s2.send_all(iter_ok::<_, Void>(10..20));
//         let send_all3 = s3.send_all(iter_ok::<_, Void>(20..30));
//         let send_all4 = s4.send_all(iter_ok::<_, Void>(30..40));
//         let sending = send_all1.join4(send_all2, send_all3, send_all4);
//
//         let (mut received, _) = receiver.collect().join(sending).wait().unwrap();
//         received.sort();
//
//         return received == (0..40).collect::<Vec<u8>>();
//     }
//
//     #[test]
//     fn test_error() {
//         let (sender, _) = test_channel::<u8, u8, Void>(8);
//         let s1 = MPS::new(TestSink::new(sender,
//                                         vec![SendOp::Delegate, SendOp::Err(13), SendOp::Delegate],
//                                         vec![]));
//         let s2 = s1.clone();
//
//         let s1 = s1.send(42).wait().unwrap();
//         assert_eq!(s2.send(42).wait().err().unwrap(), 13);
//         assert!(s1.send(42).wait().is_ok());
//     }
//
//     struct CloseTester(usize);
//
//     impl Sink for CloseTester {
//         type SinkItem = ();
//         type SinkError = ();
//
//         fn start_send(&mut self, _: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
//             Ok(AsyncSink::Ready)
//         }
//
//         fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
//             Ok(Async::Ready(()))
//         }
//
//         fn close(&mut self) -> Poll<(), Self::SinkError> {
//             self.0 += 1;
//             if self.0 > 1 {
//                 Err(())
//             } else {
//                 Ok(Async::Ready(()))
//             }
//         }
//     }
//
//     #[test]
//     fn test_close() {
//         let mut s1 = MPS::new(CloseTester(0));
//         let mut s2 = s1.clone();
//
//         assert_eq!(s1.close(), Ok(Async::Ready(())));
//
//         {
//             let s3 = s1.clone();
//             let _ = s3.flush();
//         }
//
//         assert_eq!(s2.close(), Ok(Async::Ready(())));
//     }
// }
