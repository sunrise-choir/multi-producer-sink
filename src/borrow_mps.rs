use std::cell::RefCell;

use futures::{Sink, AsyncSink, StartSend, Poll, Async};
use futures::task::{self, Task};
use ordermap::OrderSet;

use id_task::IdTask;

/// A multiple producer sink that works via lifetimes rather than reference
/// counting. This wraps a sink, and allows to obtain handles to the inner sink
/// that can use it independently. The handles can not outlive the `OwnerMPS`.
pub struct OwnerMPS<S>(RefCell<Shared<S>>);

impl<S> OwnerMPS<S> {
    /// Create a new `OwnerMPS`, wrapping the given sink.
    pub fn new(sink: S) -> OwnerMPS<S> {
        OwnerMPS(RefCell::new(Shared::new(sink)))
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

struct Shared<S> {
    inner: S,
    id_counter: usize,
    tasks: OrderSet<IdTask>,
    // The id of the handle on whose call the inner sink is currently blocking.
    // Is set to zero when inner sink is not currently blocking.
    current: usize,
}

impl<S> Shared<S> {
    fn new(sink: S) -> Shared<S> {
        Shared {
            inner: sink,
            id_counter: 1,
            tasks: OrderSet::new(),
            current: 0,
        }
    }

    fn into_inner(self) -> S {
        self.inner
    }

    fn next_id(&mut self) -> usize {
        self.id_counter += 1;
        return self.id_counter - 1;
    }
}

/// A handle for using a sink, with static lifetime checking.
pub struct Handle<'owner, S: 'owner> {
    owner: &'owner OwnerMPS<S>,
    id: usize,
}

impl<'owner, S> Handle<'owner, S> {
    /// Create a new `Handle` to the sink owned by the given owner.
    pub fn new(owner: &'owner OwnerMPS<S>) -> Handle<'owner, S> {
        let id = owner.0.borrow_mut().next_id();
        Handle { owner, id }
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
        let mut shared = self.owner.0.borrow_mut();

        // Either the inner sink is not blocking at all, or it is blocking for
        // the handle which called this method. In both scenarios we can try to
        // make progress.
        if shared.current == 0 || self.id == shared.current {
            match shared.inner.start_send(item) {
                Ok(AsyncSink::Ready) => {
                    // Done with this item. If further tasks are waiting, notify
                    // the next one.
                    shared.current = 0;

                    let front;
                    match shared.tasks.get_index(0) {
                        None => {
                            return Ok(AsyncSink::Ready);
                        }
                        Some(id_t) => {
                            front = id_t.clone();
                        }
                    }

                    front.task.notify();
                    shared.tasks.remove(&front);
                    return Ok(AsyncSink::Ready);
                }

                Ok(AsyncSink::NotReady(it)) => {
                    // Inner sink is now (and may have already been) blocking on this handle.
                    shared.current = self.id;
                    Ok(AsyncSink::NotReady(it))
                }

                Err(e) => {
                    shared.current = 0;
                    Err(e)
                }
            }
        } else {
            // Can not make progress, enqueue the task.
            shared
                .tasks
                .insert(IdTask::new(task::current(), self.id));
            Ok(AsyncSink::NotReady(item))
        }
    }

    /// Tries to poll for completion on the underlying sink. Enqueues the task
    /// if the the sink is already blocking for another `Handle`.
    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        let mut shared = self.owner.0.borrow_mut();

        // Either the inner sink is not blocking at all, or it is blocking for
        // the handle which called this method. In both scenarios we can try to
        // make progress.
        if shared.current == 0 || self.id == shared.current {
            match shared.inner.poll_complete() {
                Ok(Async::Ready(())) => {
                    // Done flushing. If further tasks are waiting, notify
                    // the next one.
                    shared.current = 0;

                    let front;
                    match shared.tasks.get_index(0) {
                        None => {
                            return Ok(Async::Ready(()));
                        }
                        Some(id_t) => {
                            front = id_t.clone();
                        }
                    }

                    front.task.notify();
                    shared.tasks.remove(&front);
                    return Ok(Async::Ready(()));
                }

                Ok(Async::NotReady) => {
                    // Inner sink is now (and may have already been) blocking on this handle.
                    shared.current = self.id;
                    Ok(Async::NotReady)
                }

                Err(e) => {
                    shared.current = 0;
                    Err(e)
                }
            }
        } else {
            // Can not make progress, enqueue the task.
            shared
                .tasks
                .insert(IdTask::new(task::current(), self.id));
            Ok(Async::NotReady)
        }
    }

    /// This simply delegates to `poll_complete`, but never actually `close`s
    /// the underlying sink. To close it, use `into_inner` on the `OwnerMPS` and
    /// then directly invoke `close` on the sink itself.
    fn close(&mut self) -> Poll<(), Self::SinkError> {
        self.poll_complete()
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
    use atm_async_utils::sink_futures::Close;

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
        let sending = send_all1
            .join4(send_all2, send_all3, send_all4)
            .and_then(|_| Close::new(owner.into_inner()));

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
}
