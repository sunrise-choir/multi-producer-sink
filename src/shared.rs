use std::hash::{Hash, Hasher};

use indexmap::IndexSet;
use futures_core::{Poll, Async};
use futures_core::task::{Waker, Context};
use futures_sink::Sink;
use futures_channel::oneshot::Sender;

pub struct Shared<S> {
    inner: Option<S>,
    sender: Option<Sender<Result<S, S>>>,
    // Counter to provide unique ids to each handle (because we can't check
    // `Waker`s for equality directly), see https://github.com/alexcrichton/futures-rs/issues/670
    id_counter: usize,
    // The number of the currently active handles that have called `close`.
    // When a handle goes out of scope, it decrements this counter if it has
    // previously incremented it (by calling `do_close`).
    // A call to `do_close` only delegates to the inner sink if the number of
    // active handles equals the close count.
    pub close_count: usize,
    // queue of wakers that tried to send/flush while the inner sink was blocking
    wakers: IndexSet<IdWaker>,
    // The id of the handle on whose call the inner sink is currently blocking.
    // Is set to zero when inner sink is not currently blocking.
    current: usize,
    errored: bool,
}

impl<S> Shared<S> {
    pub fn new(sink: S, sender: Sender<Result<S, S>>) -> Shared<S> {
        Shared {
            inner: Some(sink),
            sender: Some(sender),
            id_counter: 2,
            close_count: 0,
            wakers: IndexSet::new(),
            current: 0,
            errored: false,
        }
    }

    pub fn increment_close_count(&mut self) {
        self.close_count += 1;
    }

    pub fn decrement_close_count(&mut self) {
        self.close_count -= 1;
    }

    pub fn next_id(&mut self) -> usize {
        self.id_counter += 1;
        return self.id_counter - 1;
    }
}

impl<S: Sink> Shared<S> {
    pub fn do_poll_ready(&mut self, cx: &mut Context, id: usize) -> Poll<(), Option<S::SinkError>> {
        if self.errored {
            return Err(None);
        }

        let mut inner = self.inner.take().unwrap();

        // Either the inner sink is not blocking at all, or it is blocking for
        // the handle which called this method. In both scenarios we can try to
        // make progress.
        if self.current == 0 || id == self.current {
            match inner.poll_ready(cx) {
                Ok(Async::Ready(())) => {self.wake_next(inner); Ok(Async::Ready(()))}

                Ok(Async::Pending) => {
                    // Inner sink is now (and may have already been) blocking on this handle.
                    self.current = id;
                    self.inner = Some(inner);
                    Ok(Async::Pending)
                }

                Err(err) => {
                    self.error(inner);
                    Err(Some(err))
                }
            }
        } else {
            // Can not make progress, enqueue the task.
            self.wakers.insert(IdWaker::new(cx.waker(), id));
            self.inner = Some(inner);
            Ok(Async::Pending)
        }
    }

    pub fn do_start_send(&mut self, item: S::SinkItem) -> Result<(), Option<S::SinkError>> {
        if self.errored {
            return Err(None);
        }

        let mut inner = self.inner.take().unwrap();

        match inner.start_send(item) {
            Ok(()) => {self.inner = Some(inner); Ok(())}
            Err(err) => {
                self.error(inner);
                Err(Some(err))
            }
        }
    }

    pub fn do_poll_flush(&mut self, cx: &mut Context, id: usize) -> Poll<(), Option<S::SinkError>> {
        if self.errored {
            return Err(None);
        }

        let mut inner = self.inner.take().unwrap();

        // Either the inner sink is not blocking at all, or it is blocking for
        // the handle which called this method. In both scenarios we can try to
        // make progress.
        if self.current == 0 || id == self.current {
            match inner.poll_flush(cx) {
                Ok(Async::Ready(())) => {self.wake_next(inner); Ok(Async::Ready(()))}

                Ok(Async::Pending) => {
                    // Inner sink is now (and may have already been) blocking on this handle.
                    self.current = id;
                    self.inner = Some(inner);
                    Ok(Async::Pending)
                }

                Err(err) => {
                    self.error(inner);
                    Err(Some(err))
                }
            }
        } else {
            // Can not make progress, enqueue the task.
            self.wakers.insert(IdWaker::new(cx.waker(), id));
            self.inner = Some(inner);
            Ok(Async::Pending)
        }
    }

    pub fn do_poll_close(&mut self,
                         cx: &mut Context,
                         id: usize,
                         really_close: bool)
                         -> Poll<(), Option<S::SinkError>> {
        if self.errored {
            return Err(None);
        }

        let mut inner = self.inner.take().unwrap();

        // Either the inner sink is not blocking at all, or it is blocking for
        // the handle which called this method. In both scenarios we can try to
        // make progress.
        if self.current == 0 || id == self.current {
            // close the inner sink
            if (really_close) && self.wakers.is_empty() {
                match inner.poll_close(cx) {
                    Ok(Async::Ready(())) => {
                        // Done closing.
                        self.current = 0;

                        self.wakers.pop();

                        let _ = self.sender.take().unwrap().send(Ok(inner));
                        Ok(Async::Ready(()))
                    }

                    Ok(Async::Pending) => {
                        // Inner sink is now (and may have already been) blocking on this handle.
                        self.current = id;
                        self.inner = Some(inner);
                        Ok(Async::Pending)
                    }

                    Err(e) => {
                        self.error(inner);
                        Err(Some(e))
                    }
                }

                // only flush
            } else {
                match inner.poll_flush(cx) {
                    Ok(Async::Ready(())) => {self.wake_next(inner); Ok(Async::Ready(()))}

                    Ok(Async::Pending) => {
                        // Inner sink is now (and may have already been) blocking on this handle.
                        self.current = id;
                        self.inner = Some(inner);
                        Ok(Async::Pending)
                    }

                    Err(err) => {
                        self.error(inner);
                        Err(Some(err))
                    }
                }
            }
        } else {
            // Can not make progress, enqueue the task.
            self.wakers.insert(IdWaker::new(cx.waker(), id));
            self.inner = Some(inner);
            Ok(Async::Pending)
        }
    }

    fn wake(&mut self) {
        let front;
        match self.wakers.get_index(0) {
            None => {
                return;
            }
            Some(id_w) => {
                front = id_w.clone();
            }
        }
        front.waker.wake();
        self.wakers.remove(&front);
    }

    fn error(&mut self, inner: S) {
        self.errored = true;
        self.current = 0;
        let _ = self.sender.take().unwrap().send(Err(inner));
        self.wake();
    }

    fn wake_next(&mut self, inner: S) {
        self.current = 0;
        self.inner = Some(inner);
        self.wake();
    }
}

#[derive(Clone)]
struct IdWaker {
    pub waker: Waker,
    pub id: usize,
}

impl IdWaker {
    fn new(waker: Waker, id: usize) -> IdWaker {
        IdWaker { waker, id }
    }
}

impl Hash for IdWaker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

impl PartialEq for IdWaker {
    fn eq(&self, rhs: &IdWaker) -> bool {
        self.id == rhs.id
    }
}

impl Eq for IdWaker {}
