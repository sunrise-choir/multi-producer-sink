//! A single-threaded multi-producer-sink.

use std::cell::RefCell;
use std::rc::Rc;

use futures_core::{Future, Poll};
use futures_channel::oneshot::{channel, Sender, Receiver, Canceled};
use futures_sink::Sink;
use futures_util::FutureExt;
use futures_util::future::Then;
use futures_core::task::Context;

use shared::*;

/// Create a new MPS, wrapping the given sink. Also returns a future that
/// emits the wrapped sink once the last `MPS` handle has been closed, dropped or if the wrapped
/// sink errored.
pub fn mps<S>(sink: S) -> (MPS<S>, Done<S>) {
    let (done, sender) = Done::new();

    (MPS {
         shared: Rc::new(RefCell::new(Shared::new(sink, sender))),
         did_close: false,
         id: 1,
     },
     done)
}

/// A future that signals when the wrapped sink is done.
///
/// Yields back the wrapped sink in an `Ok` when the last handle is closed or dropped.
/// Emits the wrapped sink as an `Err` if it errors.
pub struct Done<S>(Then<Receiver<Result<S, S>>,
                         Result<S, S>,
                         fn(Result<Result<S, S>, Canceled>) -> Result<S, S>>);

impl<S> Done<S> {
    fn new() -> (Done<S>, Sender<Result<S, S>>) {
        let (sender, receiver) = channel();

        (Done(receiver.then(|result| match result {
                                Ok(Ok(sink)) => Ok(sink),
                                Ok(Err(sink)) => Err(sink),
                                Err(_) => unreachable!(),
                            })),
         sender)
    }
}

impl<S> Future for Done<S> {
    type Item = S;
    type Error = S;

    fn poll(&mut self, cx: &mut Context) -> Poll<Self::Item, Self::Error> {
        self.0.poll(cx)
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

    fn poll_ready(&mut self, cx: &mut Context) -> Poll<(), Option<S::SinkError>> {
        self.shared.borrow_mut().do_poll_ready(cx, self.id)
    }

    fn start_send(&mut self, item: S::SinkItem) -> Result<(), Option<S::SinkError>> {
        self.shared.borrow_mut().do_start_send(item)
    }

    fn poll_flush(&mut self, cx: &mut Context) -> Poll<(), Self::SinkError> {
        self.shared.borrow_mut().do_poll_flush(cx, self.id)
    }

    /// This only delegates to the `poll_close` method of the inner sink if all other
    /// active handles have already called close. Else, it simply flushes the
    /// underlying sink, but does not close it.
    ///
    /// Calling `clone` after calling `close` leads to ambiguity whether the
    /// inner sink has actually closed yet. It's still safe, but not advisable.
    fn poll_close(&mut self, cx: &mut Context) -> Poll<(), Self::SinkError> {
        let mut shared = self.shared.borrow_mut();
        let mut close_count = shared.close_count;

        if !self.did_close {
            shared.increment_close_count();
            close_count += 1;
            self.did_close = true;
        }

        shared.do_poll_close(cx, self.id, Rc::strong_count(&self.shared) == close_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use atm_async_utils::test_channel::*;
    use futures::{SinkExt, StreamExt, FutureExt, Never};
    use futures::sink::close;
    use futures::stream::iter_ok;
    use futures::executor::block_on;

    #[test]
    fn test_success() {
        let (sender, receiver) = test_channel::<u8, Never>(2);

        let (s1, done) = mps(sender);
        let s2 = s1.clone();
        let s3 = s1.clone();
        let s4 = s1.clone();

        let send_all1 = s1.send_all(iter_ok::<_, Never>(0..10).map(|i| Ok(i)))
            .and_then(|(sender, _)| close(sender));
        let send_all2 = s2.send_all(iter_ok::<_, Never>(10..20).map(|i| Ok(i)))
            .and_then(|(sender, _)| close(sender));
        let send_all3 = s3.send_all(iter_ok::<_, Never>(20..30).map(|i| Ok(i)))
            .and_then(|(sender, _)| close(sender));
        let send_all4 = s4.send_all(iter_ok::<_, Never>(30..40).map(|i| Ok(i)))
            .and_then(|(sender, _)| close(sender));
        let sending = send_all1
            .join4(send_all2, send_all3, send_all4)
            .map_err(|_| unreachable!());

        let (mut received, _, _) = block_on(receiver
                                                .collect()
                                                .join3(sending,
                                                       done.map_err(|_| unreachable!())))
                .unwrap();

        received.sort();
        assert_eq!(received, (0..40).collect::<Vec<u8>>());
    }
}
