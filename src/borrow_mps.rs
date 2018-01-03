use std::cell::RefCell;

use futures::{Sink, StartSend, Poll};

use shared::*;

/// A multiple producer sink that works via lifetimes rather than reference
/// counting. This wraps a sink, and allows to obtain handles to the inner sink
/// that can use it independently. The handles can not outlive the `OwnerMPS`.
pub struct OwnerMPS<S>(RefCell<Shared<S>>);

impl<S> OwnerMPS<S> {
    /// Create a new `OwnerMPS`, wrapping the given sink.
    pub fn new(sink: S) -> OwnerMPS<S> {
        OwnerMPS(RefCell::new(Shared::new(sink, 1, 0)))
    }

    /// Consume the `OwnerMPS` and return ownership of the inner sink.
    ///
    /// The only way of closing the inner sink is by using this method and then
    /// calling `close` on the sink itself.
    pub fn into_inner(self) -> S {
        self.0.into_inner().into_inner()
    }

    /// Create a new handle to the underlying sink. This handle implements the
    /// `Sink` trait and delegates to the inner sink. It can however not close
    /// the inner sink.
    pub fn handle(&self) -> Handle<S> {
        Handle::new(self)
    }
}

/// A handle for using a sink, with static lifetime checking.
pub struct Handle<'owner, S: 'owner> {
    owner: &'owner OwnerMPS<S>,
    did_close: bool,
    id: usize,
}

impl<'owner, S> Handle<'owner, S> {
    /// Create a new `Handle` to the sink owned by the given owner.
    pub fn new(owner: &'owner OwnerMPS<S>) -> Handle<'owner, S> {
        let mut shared = owner.0.borrow_mut();
        let id = shared.next_id();
        shared.increment_ref_count();

        Handle {
            owner,
            did_close: false,
            id,
        }
    }
}

/// Performs minimal cleanup to allow for correct closing behaviour
impl<'owner, S> Drop for Handle<'owner, S> {
    fn drop(&mut self) {
        let mut shared = self.owner.0.borrow_mut();
        if self.did_close {
            shared.decrement_close_count();
        }

        shared.decrement_ref_count();
    }
}

impl<'owner, S> Clone for Handle<'owner, S> {
    /// This is the same as calling `handle` on the `OwnerMPS` corresponding to
    /// this `Handle`.
    fn clone(&self) -> Handle<'owner, S> {
        self.owner.handle()
    }
}

impl<'owner, S: Sink> Sink for Handle<'owner, S> {
    type SinkItem = S::SinkItem;
    type SinkError = S::SinkError;

    /// Tries to start sending to the underlying sink. Enqueues the task if the
    /// the sink is already blocking for another `Handle`.
    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        self.owner.0.borrow_mut().do_start_send(item, self.id)
    }

    /// Tries to poll for completion on the underlying sink. Enqueues the task
    /// if the the sink is already blocking for another `Handle`.
    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.owner.0.borrow_mut().do_poll_complete(self.id)
    }

    /// This only delegates to the `close` method of the inner sink if all other
    /// active handles have already called close. Else, it simply flushes the
    /// underlying sink, but does not close it.
    ///
    /// Calling `master.handle()` or `handle.clone()` after calling `close` leads
    /// to ambiguity whether the inner sink has actually closed yet. It's still
    /// safe, but not advisable.
    fn close(&mut self) -> Poll<(), Self::SinkError> {
        let mut shared = self.owner.0.borrow_mut();

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

        let owner = OwnerMPS::new(sender);

        let s1 = owner.handle();
        let s2 = owner.handle();
        let s3 = owner.handle();
        let s4 = owner.handle();

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
        let owner =
            OwnerMPS::new(TestSink::new(sender,
                                        vec![SendOp::Delegate, SendOp::Err(13), SendOp::Delegate],
                                        vec![]));
        let s1 = owner.handle();
        let s2 = owner.handle();

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
        let owner = OwnerMPS::new(CloseTester(0));
        let mut s1 = owner.handle();
        let mut s2 = owner.handle();

        assert_eq!(s1.close(), Ok(Async::Ready(())));

        {
            let s3 = owner.handle();
            let _ = s3.flush();
        }

        assert_eq!(s2.close(), Ok(Async::Ready(())));
    }
}
