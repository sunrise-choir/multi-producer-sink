use std::hash::{Hash, Hasher};

use futures::{Sink, AsyncSink, StartSend, Poll, Async};
use futures::task::{self, Task};
use ordermap::OrderSet;

pub struct Shared<S> {
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
    // The number of currently active handles.
    ref_count: usize,
    // queue of tasks that tried to send/flush while the inner sink was blocking
    tasks: OrderSet<IdTask>,
    // The id of the handle on whose call the inner sink is currently blocking.
    // Is set to zero when inner sink is not currently blocking.
    current: usize,
}

impl<S> Shared<S> {
    pub fn new(sink: S, initial_id_counter: usize, initial_ref_count: usize) -> Shared<S> {
        Shared {
            inner: sink,
            id_counter: initial_id_counter,
            close_count: 0,
            ref_count: initial_ref_count,
            tasks: OrderSet::new(),
            current: 0,
        }
    }

    pub fn into_inner(self) -> S {
        self.inner
    }

    pub fn increment_close_count(&mut self) {
        self.close_count += 1;
    }

    pub fn decrement_close_count(&mut self) {
        self.close_count -= 1;
    }

    pub fn increment_ref_count(&mut self) {
        self.ref_count += 1;
    }

    pub fn decrement_ref_count(&mut self) {
        self.ref_count -= 1;
    }

    pub fn next_id(&mut self) -> usize {
        self.id_counter += 1;
        return self.id_counter - 1;
    }
}

impl<S: Sink> Shared<S> {
    pub fn do_start_send(&mut self,
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

    pub fn do_poll_complete(&mut self, id: usize) -> Poll<(), S::SinkError> {
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

    pub fn do_close(&mut self, id: usize) -> Poll<(), S::SinkError> {
        // Either the inner sink is not blocking at all, or it is blocking for
        // the handle which called this method. In both scenarios we can try to
        // make progress.
        if self.current == 0 || id == self.current {
            // close the inner sink
            if (self.ref_count == self.close_count || self.ref_count == 1) &&
               self.tasks.is_empty() {
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

#[derive(Clone)]
pub struct IdTask {
    pub task: Task,
    pub id: usize,
}

impl IdTask {
    pub fn new(task: Task, id: usize) -> IdTask {
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
