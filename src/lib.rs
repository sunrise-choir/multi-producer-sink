extern crate futures;

use std::collections::vec_deque::VecDeque;
use std::cell::UnsafeCell;
use std::mem;
use std::ptr;

use futures::{Sink, Poll, StartSend, AsyncSink};
use futures::Async::{Ready, NotReady};
use futures::task::{Task, current};

/// A wrapper around a Sink . The `sub_sink` method can be used to obtain
/// multiple sinks all writing to the same underlying one.
// The usize is a counter to assign ids to SubSinks.
struct MainSink<S: Sink>(UnsafeCell<MainSinkImpl<S>>, usize);

impl<S: Sink> MainSink<S> {
    /// Consumes a Sink and returns a corresponding MainSink.
    pub fn new(sink: S) -> MainSink<S> {
        MainSink(UnsafeCell::new(MainSinkImpl::new(sink)), 0)
    }

    /// Create a new SubSink which writes to the underlying Sink of this MainSink.
    pub fn sub_sink(&mut self) -> SubSink<S> {
        let id = self.1;
        self.1 += 1;
        SubSink(self, id)
    }

    /// Consumes the `MainSink` and returns ownership of the underlying sink.
    /// If the sink has errored, this also returns ownership of the error.
    pub fn into_inner(self) -> (S, Option<S::SinkError>) {
        let main_sink_impl = unsafe { self.0.into_inner() };
        (main_sink_impl.sink,
         if main_sink_impl.did_error {
             Some(main_sink_impl.error)
         } else {
             None
         })
    }

    // Invoked by the SubSink, unlike the Sink trait method, this does not require
    // a mutable reference.
    fn start_send(&self, item: S::SinkItem, id: usize) -> StartSend<S::SinkItem, &S::SinkError> {
        let inner = unsafe { &mut *self.0.get() };
        inner.start_send(item, id)
    }

    // Invoked by the SubSink, unlike the Sink trait method, this does not require
    // a mutable reference.
    fn poll_complete(&self, id: usize) -> Poll<(), &S::SinkError> {
        let inner = unsafe { &mut *self.0.get() };
        inner.poll_complete(id)
    }
}

struct MainSinkImpl<S: Sink> {
    sink: S,
    // Populated with the first error happening over the inner sink.
    error: S::SinkError,
    // In order to return direct references to the first error, the error is not
    // stored in an Option, but simply as a field. That field is unsafely initialized,
    // and the `did_error` flag is used to check whether the error field contains
    // a valid error.
    did_error: bool,
    // Tasks blocked on `start_send`, with the id of their SubSink.
    send_tasks: VecDeque<(Task, usize)>,
    // Tasks blocked on `poll_complete`, with the id of their SubSink.
    complete_tasks: VecDeque<(Task, usize)>,
}

impl<S: Sink> MainSinkImpl<S> {
    /// Consumes a Sink and returns a corresponding MainSink.
    pub fn new(sink: S) -> MainSinkImpl<S> {
        MainSinkImpl {
            sink,
            error: unsafe { mem::uninitialized() },
            did_error: false,
            send_tasks: VecDeque::new(),
            complete_tasks: VecDeque::new(),
        }
    }

    /// Consumes the `MainSink` and returns ownership of the underlying sink.
    /// If the sink has errored, this also returns ownership of the error.
    pub fn into_inner(self) -> (S, Option<S::SinkError>) {
        (self.sink,
         if self.did_error {
             Some(self.error)
         } else {
             None
         })
    }

    // Only use this for setting this.error, everything else is undefined behaviour.
    fn set_error(&mut self, e: S::SinkError) {
        unsafe { ptr::write(&mut self.error as *mut S::SinkError, e) };

        self.did_error = true;
    }

    fn start_send(&mut self,
                  item: S::SinkItem,
                  id: usize)
                  -> StartSend<S::SinkItem, &S::SinkError> {
        if self.did_error {
            // After the first error, stop normal operation.
            Err(&self.error)
        } else {
            // We haven't errored yet, so check whether we are blocking on anything.
            if self.complete_tasks.is_empty() {
                // Due to multiple mutable references, we can not simply match on
                // `self.send_tasks.get(0)`, so we do a little dance to appease
                // the compiler.
                let mut is_empty;
                let mut front_id;
                {
                    let front = self.send_tasks.get(0);
                    is_empty = front.is_none();
                    front_id = front.map_or(0, |pair| pair.1);
                }

                if is_empty {
                    // Underlying sink is not blocking at all.
                    match self.sink.start_send(item) {
                        Ok(AsyncSink::Ready) => {
                            return Ok(AsyncSink::Ready);
                        }
                        Ok(AsyncSink::NotReady(item)) => {
                            self.send_tasks.push_back((current(), id));
                            return Ok(AsyncSink::NotReady(item));
                        }
                        Err(e) => {
                            self.set_error(e);
                            return Err(&self.error);
                        }
                    }
                } else {
                    // Underlying sink is blocking on poll_send.
                    if id == front_id {
                        // The underlying sink woke the task, so we can try again.
                        match self.sink.start_send(item) {
                            Ok(AsyncSink::Ready) => {
                                let _ = self.send_tasks.pop_front();
                                return Ok(AsyncSink::Ready);
                            }
                            Ok(AsyncSink::NotReady(item)) => {
                                return Ok(AsyncSink::NotReady(item));
                            }
                            Err(e) => {
                                let _ = self.send_tasks.pop_front();
                                self.set_error(e);
                                return Err(&self.error);
                            }
                        }
                    } else {
                        // Blocking on another send task
                        self.send_tasks.push_back((current(), front_id));
                        return Ok(AsyncSink::NotReady(item));
                    }
                }
            } else {
                // The underlying sink is blocking on poll_complete.
                self.send_tasks.push_back((current(), id));
                return Ok(AsyncSink::NotReady(item));
            }
        }
    }

    // Invoked by the SubSink, unlike the Sink trait method, this does not require
    // a mutable reference.
    fn poll_complete(&mut self, id: usize) -> Poll<(), &S::SinkError> {
        if self.did_error {
            Err(&self.error)
        } else {

            // TODO
            unimplemented!()
        }
    }
}

// The usize is an id used to match enqueued tasks.
struct SubSink<'s, S: 's + Sink>(&'s MainSink<S>, usize);

impl<'s, S: Sink> Sink for SubSink<'s, S> {
    type SinkItem = S::SinkItem;
    /// Errors are references to the first error that occured on the underlying Sink.
    /// Once an error happend, start_send and poll_complete will always return a
    /// reference to that error, without performing any other action.
    type SinkError = &'s S::SinkError;

    /// Start sending on the underlying Sink.
    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        self.0.start_send(item, self.1)
    }

    /// Poll for completion on the underlying Sink.
    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.0.poll_complete(self.1)
    }

    /// Closing a SubSink is a no-op. To actually close the underlying Sink,
    /// consume the MainSink via `into_inner()` and then invoke close on the Sink
    /// itself.
    fn close(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Ready(())) // no-op
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
