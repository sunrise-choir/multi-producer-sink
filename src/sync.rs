//! A threadsafe multi-producer-sink.

use std::sync::{Arc, RwLock};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;

use futures_core::{Future, Poll};
use futures_channel::oneshot::{channel, Sender, Receiver, Canceled};
use futures_sink::Sink;
use futures_util::FutureExt;
use futures_util::future::Then;
use futures_core::task::Context;

use shared::*;
use super::MPS;

/// Create a new SyncMPS, wrapping the given sink. Also returns a future that
/// emits the wrapped sink once the last `SyncMPS` handle has been closed, dropped or if the wrapped
/// sink errored.
pub fn sync_mps<S: Sink>(sink: S) -> (SyncMPS<S>, SyncDone<S>) {
    let (done, sender) = SyncDone::new();

    (SyncMPS {
         shared: Arc::new(RwLock::new(Shared::new(sink, sender))),
         did_close: AtomicBool::new(false),
         id: 1,
     },
     done)
}

/// A future that signals when the wrapped sink is done.
///
/// Yields back the wrapped sink in an `Ok` when the last handle is closed or dropped.
/// Emits the first error and the wrapped sink as an `Err` if the sink errors.
pub struct SyncDone<S: Sink>(Then<Receiver<Result<S, (S::SinkError, S)>>,
                                   Result<S, (S::SinkError, S)>,
                                   fn(Result<Result<S, (S::SinkError, S)>, Canceled>)
                                      -> Result<S, (S::SinkError, S)>>);

impl<S: Sink> SyncDone<S> {
    fn new() -> (SyncDone<S>, Sender<Result<S, (S::SinkError, S)>>) {
        let (sender, receiver) = channel();

        (SyncDone(receiver.then(|result| match result {
                                    Ok(Ok(sink)) => Ok(sink),
                                    Ok(Err((err, sink))) => Err((err, sink)),
                                    Err(_) => unreachable!(),
                                })),
         sender)
    }
}

impl<S: Sink> Future for SyncDone<S> {
    type Item = S;
    type Error = (S::SinkError, S);

    fn poll(&mut self, cx: &mut Context) -> Poll<Self::Item, Self::Error> {
        self.0.poll(cx)
    }
}

/// A multi producer sink (`SyncMPS`). This is a cloneable handle to a single
/// sink of type `S`, and each handle can be used to write to the inner sink.
///
/// An error is signaled via the `Done`, the sink methods themselves only return `Err(())`. Upon
/// encountering an error, all handles are notified and they return `Err(())`. All further polling
/// will always yield `Err(None)` as well.
///
/// Unless an error occured, each of the handles must invoke `close` before being dropped. The
/// inner sink is closed when each of the handles has `close`d and emitted via the `Done`.
pub struct SyncMPS<S: Sink> {
    shared: Arc<RwLock<Shared<S>>>,
    did_close: AtomicBool,
    // id may never be 0, 0 signals that nothing is blocking
    id: usize,
}

/// Performs minimal cleanup to allow for correct closing behaviour
impl<S: Sink> Drop for SyncMPS<S> {
    fn drop(&mut self) {
        if self.did_close.load(SeqCst) {
            self.shared.write().unwrap().decrement_close_count();
        }
    }
}

impl<S: Sink> Clone for SyncMPS<S> {
    /// Returns a new handle to the same underlying sink.
    fn clone(&self) -> SyncMPS<S> {
        SyncMPS {
            shared: self.shared.clone(),
            did_close: AtomicBool::new(false),
            id: self.shared.write().unwrap().next_id(),
        }
    }
}

impl<S: Sink> Sink for SyncMPS<S> {
    type SinkItem = S::SinkItem;
    type SinkError = ();

    fn poll_ready(&mut self, cx: &mut Context) -> Poll<(), Self::SinkError> {
        self.shared.write().unwrap().do_poll_ready(cx, self.id)
    }

    fn start_send(&mut self, item: S::SinkItem) -> Result<(), Self::SinkError> {
        self.shared.write().unwrap().do_start_send(item)
    }

    fn poll_flush(&mut self, cx: &mut Context) -> Poll<(), Self::SinkError> {
        self.shared.write().unwrap().do_poll_flush(cx, self.id)
    }

    /// This only delegates to the `poll_close` method of the inner sink if all other
    /// active handles have already called close. Else, it simply flushes the
    /// underlying sink, but does not close it.
    ///
    /// Calling `clone` after calling `close` leads to ambiguity whether the
    /// inner sink has actually closed yet. It's still safe, but not advisable.
    fn poll_close(&mut self, cx: &mut Context) -> Poll<(), Self::SinkError> {
        let mut shared = self.shared.write().unwrap();
        let mut close_count = shared.close_count;

        if !self.did_close.load(SeqCst) {
            shared.increment_close_count();
            close_count += 1;
            self.did_close.store(true, SeqCst);
        }

        shared.do_poll_close(cx, self.id, Arc::strong_count(&self.shared) == close_count)
    }
}

impl<S: Sink> MPS<S> for SyncMPS<S> {
    type Done = SyncDone<S>;

    fn mps(sink: S) -> (Self, Self::Done) {
        sync_mps(sink)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use atm_async_utils::test_channel::*;
    use futures::{SinkExt, StreamExt, FutureExt, Never};
    use futures::sink::close;
    use futures::stream::iter_ok;
    use futures::executor::ThreadPool;

    #[test]
    fn test_success() {
        let (sender, receiver) = test_channel::<u8, Never>(2);

        let (s1, done) = sync_mps(sender);
        let s2 = s1.clone();
        let s3 = s1.clone();
        let s4 = s1.clone();

        let send_all1 = s1.send_all(iter_ok::<_, ()>(0..10).map(|i| Ok(i)))
            .and_then(|(sender, _)| close(sender));
        let send_all2 = s2.send_all(iter_ok::<_, ()>(10..20).map(|i| Ok(i)))
            .and_then(|(sender, _)| close(sender));
        let send_all3 = s3.send_all(iter_ok::<_, ()>(20..30).map(|i| Ok(i)))
            .and_then(|(sender, _)| close(sender));
        let send_all4 = s4.send_all(iter_ok::<_, ()>(30..40).map(|i| Ok(i)))
            .and_then(|(sender, _)| close(sender));
        let sending = send_all1
            .join4(send_all2, send_all3, send_all4)
            .map_err(|_| unreachable!());

        assert!(ThreadPool::new()
                    .run(receiver
                             .collect()
                             .map(|mut received| {
                                      received.sort();
                                      assert_eq!(received, (0..40).collect::<Vec<u8>>())
                                  })
                             .join3(sending, done.map_err(|_| unreachable!())))
                    .is_ok());
    }

    fn is_send<T: Send>(_: T) {
        ()
    }

    #[test]
    fn implements_send() {
        let (sender, _) = test_channel::<u8, Never>(2);
        let (s, done) = sync_mps(sender);

        is_send(s);
        is_send(done)
    }
}
