use std::cell::RefCell;
use std::rc::Rc;

use futures::{Sink, StartSend, Poll};

use shared::*;

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
            shared: Rc::new(RefCell::new(Shared::new(sink, 2, 1))),
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
        self.shared.borrow_mut().increment_ref_count();
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
            shared.increment_close_count();
            self.did_close = true;
        }

        shared.do_close(self.id)
    }
}

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
