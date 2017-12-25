extern crate futures;

#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
extern crate rand;
#[cfg(test)]
extern crate void;
#[cfg(test)]
extern crate atm_async_utils;

use std::collections::HashMap;
use std::cell::{Cell, UnsafeCell};
use std::mem;
use std::ptr;

use futures::{Sink, Poll, StartSend, AsyncSink, Async};
use futures::task::{Task, current};

/// A wrapper around a Sink . The `sub_sink` method can be used to obtain
/// multiple sinks all writing to the same underlying one.
// The usize is a counter to assign ids to SubSinks.
pub struct MainSink<S: Sink>(UnsafeCell<MainSinkImpl<S>>, Cell<usize>);

impl<S: Sink> MainSink<S>
    where <S as futures::Sink>::SinkItem: std::fmt::Debug
{
    /// Consumes a Sink and returns a corresponding MainSink.
    pub fn new(sink: S) -> MainSink<S> {
        MainSink(UnsafeCell::new(MainSinkImpl::new(sink)), Cell::new(0))
    }

    /// Create a new SubSink which writes to the underlying Sink of this MainSink.
    pub fn sub_sink(&self) -> SubSink<S> {
        let old_id = self.1.get();
        self.1.set(old_id + 1);
        SubSink(self, old_id)
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

    // Invoked by the SubSink, unlike the Sink trait method, this does not require
    // a mutable reference.
    fn close(&self) -> Poll<(), &S::SinkError> {
        let inner = unsafe { &mut *self.0.get() };
        inner.close()
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
    // Id and task of the SubSink that caused the currently blocking call. None when not blocking.
    current: Option<(usize, Task)>,
    // Ids and Tasks of all other SubSinks that want to use the sink but can't since it is currently
    // blocking.
    tasks: HashMap<usize, Task>,
}

impl<S: Sink> MainSinkImpl<S>
    where <S as futures::Sink>::SinkItem: std::fmt::Debug
{
    /// Consumes a Sink and returns a corresponding MainSink.
    pub fn new(sink: S) -> MainSinkImpl<S> {
        MainSinkImpl {
            sink,
            error: unsafe { mem::uninitialized() },
            did_error: false,
            current: None,
            tasks: HashMap::new(),
        }
    }

    // Only use this for setting this.error, everything else is undefined behaviour.
    fn set_error(&mut self, e: S::SinkError) {
        unsafe { ptr::write(&mut self.error as *mut S::SinkError, e) };

        self.did_error = true;

        for (_, task) in self.tasks.iter() {
            task.notify();
        }
    }

    fn start_send(&mut self,
                  item: S::SinkItem,
                  id: usize)
                  -> StartSend<S::SinkItem, &S::SinkError> {
        println!("impl start_send, id: {}, item: {:?}", id, item);
        println!("tasks: {:?}", self.tasks);
        if self.did_error {
            // After the first error, stop normal operation.
            Err(&self.error)
        } else {
            println!("send: not errored");

            match self.current {
                None => {
                    // Not blocking on anything, use the sink.
                    println!("send: not blocking on anything");
                    match self.sink.start_send(item) {
                        Ok(AsyncSink::Ready) => {
                            println!("send: sent to inner");
                            return Ok(AsyncSink::Ready);
                        }
                        Ok(AsyncSink::NotReady(item)) => {
                            println!("send: blocking on inner send, own id: {}", id);
                            self.current = Some((id, current()));
                            // self.tasks.insert(id, current());
                            return Ok(AsyncSink::NotReady(item));
                        }
                        Err(e) => {
                            self.set_error(e);
                            return Err(&self.error);
                        }
                    }
                }
                Some((current_id, _)) if current_id == id => {
                    println!("Send: blocking on own id: {}", id);
                    // Blocking on this task, try it again
                    match self.sink.start_send(item) {
                        Ok(AsyncSink::Ready) => {
                            self.current = None;
                            println!("removed task: {}", id);

                            // notify the next task
                            let next = self.tasks.iter().next();
                            match next {
                                None => {
                                    // noop, nothing needs to be notified
                                    println!("no further tasks to notify");
                                }
                                Some((id, task)) => {
                                    task.notify();
                                    println!("notified {}", id);
                                }
                            }

                            return Ok(AsyncSink::Ready);
                        }
                        Ok(AsyncSink::NotReady(item)) => {
                            println!("still blocking for {}", id);
                            return Ok(AsyncSink::NotReady(item));
                        }
                        Err(e) => {
                            self.current = None;
                            self.set_error(e);
                            return Err(&self.error);
                        }
                    }
                }
                Some((current_id, ref task)) => {
                    println!("send: blocking on other id: {}", current_id);
                    // Blocking on some other task, notify it and park this one
                    if self.tasks.insert(id, current()).is_none() {
                        println!("parked {}", id);
                        return Ok(AsyncSink::NotReady(item));
                    } else {
                        // This call was triggered because inner sink notified the task, so wake up
                        // the current one
                        task.notify();
                        println!("notified current");
                        return Ok(AsyncSink::NotReady(item));
                    }
                }
            }
        }
    }

    // Invoked by the SubSink, unlike the Sink trait method, this does not require
    // a mutable reference.
    fn poll_complete(&mut self, id: usize) -> Poll<(), &S::SinkError> {
        println!("impl poll_complete, id: {}", id);
        println!("tasks: {:?}", self.tasks);
        if self.did_error {
            // After the first error, stop normal operation.
            Err(&self.error)
        } else {
            println!("complete: not errored");

            match self.current {
                None => {
                    // Not blocking on anything, use the sink.
                    println!("complete: not blocking on anything");
                    match self.sink.poll_complete() {
                        Ok(Async::Ready(_)) => {
                            println!("complete: completed inner");
                            return Ok(Async::Ready(()));
                        }
                        Ok(Async::NotReady) => {
                            println!("complete: blocking on inner complete, own id: {}", id);
                            self.current = Some((id, current()));
                            return Ok(Async::NotReady);
                        }
                        Err(e) => {
                            self.set_error(e);
                            return Err(&self.error);
                        }
                    }
                }
                Some((current_id, _)) if current_id == id => {
                    println!("complete: blocking on own id: {}", id);
                    // Blocking on this task, try it again
                    match self.sink.poll_complete() {
                        Ok(Async::Ready(_)) => {
                            self.current = None;
                            println!("removed task: {}", id);

                            // notify the next task
                            let next = self.tasks.iter().next();
                            match next {
                                None => {
                                    // noop, nothing needs to be notified
                                    println!("no further tasks to notify");
                                }
                                Some((id, task)) => {
                                    task.notify();
                                    println!("notified {}", id);
                                }
                            }

                            return Ok(Async::Ready(()));
                        }
                        Ok(Async::NotReady) => {
                            println!("still blocking for {}", id);
                            return Ok(Async::NotReady);
                        }
                        Err(e) => {
                            self.current = None;
                            self.set_error(e);
                            return Err(&self.error);
                        }
                    }
                }
                Some((current_id, ref task)) => {
                    println!("complete: blocking on other id: {}", current_id);
                    // Blocking on some other task, notify it and park this one
                    if self.tasks.insert(id, current()).is_none() {
                        println!("parked {}", id);
                        return Ok(Async::NotReady);
                    } else {
                        // This call was triggered because inner sink notified the task, so wake up
                        // the current one
                        task.notify();
                        println!("notified current");
                        return Ok(Async::NotReady);
                    }
                }
            }
        }
    }

    fn close(&mut self) -> Poll<(), &S::SinkError> {
        println!("impl close");
        match self.sink.close() {
            Ok(x) => Ok(x),
            Err(e) => {
                self.set_error(e);
                return Err(&self.error);
            }
        }
    }
}

// The usize is an id used to match enqueued tasks.
pub struct SubSink<'s, S: 's + Sink>(&'s MainSink<S>, usize);

impl<'s, S: Sink> Sink for SubSink<'s, S>
    where <S as futures::Sink>::SinkItem: std::fmt::Debug
{
    type SinkItem = S::SinkItem;
    /// Errors are references to the first error that occured on the underlying Sink.
    /// Once an error happend, start_send and poll_complete will always return a
    /// reference to that error, without performing any other action.
    type SinkError = &'s S::SinkError;

    /// Start sending on the underlying Sink.
    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        println!("");
        println!("subsink {}: start_send {:?}", self.1, item);
        self.0.start_send(item, self.1)
    }

    /// Poll for completion on the underlying Sink.
    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        println!("subsink {}: poll_complete", self.1);
        self.0.poll_complete(self.1)
    }

    /// Closing a SubSink closes the underlying sink. Some sinks may panic if
    /// they are being written to after closing, so care must be taken when
    /// closing this while other SubSinks are still active.
    fn close(&mut self) -> Poll<(), Self::SinkError> {
        println!("subsink {}: close", self.1);
        self.0.close()
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
    use atm_async_utils::sink_futures::{Close, SendAll};

    #[test]
    fn test_errors() {
        let (sender, _) = test_channel::<u8, u8, Void>(8);
        let main =
            MainSink::new(TestSink::new(sender, vec![SendOp::Delegate, SendOp::Err(13)], vec![]));
        let s1 = main.sub_sink();
        let s2 = main.sub_sink();

        let s1 = s1.send(42).wait().unwrap();
        assert_eq!(s2.send(42).wait().err().unwrap(), &13);
        assert_eq!(s1.send(42).wait().err().unwrap(), &13);
    }

    #[test]
    fn test_success() {
        let rng = StdGen::new(rand::thread_rng(), 50);
        let mut quickcheck = QuickCheck::new().gen(rng).tests(1000);
        quickcheck.quickcheck(success as fn(usize) -> bool);
    }

    fn success(buf_size: usize) -> bool {
        let (sender, receiver) = test_channel::<u8, Void, Void>(buf_size + 1);

        let main = MainSink::new(sender);

        let s1 = main.sub_sink();
        let s2 = main.sub_sink();
        let s3 = main.sub_sink();
        let s4 = main.sub_sink();

        let send_all1 = SendAll::new(s1, iter_ok::<_, &Void>(0..10));
        let send_all2 = SendAll::new(s2, iter_ok::<_, &Void>(10..20));
        let send_all3 = SendAll::new(s3, iter_ok::<_, &Void>(20..30));
        let send_all4 = SendAll::new(s4, iter_ok::<_, &Void>(30..40));
        let sending = send_all1
            .join4(send_all2, send_all3, send_all4)
            .map_err(|x| *x)
            .and_then(|(_, _, _, (s4, _))| Close::new(s4).map_err(|x| *x));

        let (mut received, _) = receiver.collect().join(sending).wait().unwrap();
        received.sort();

        return received == (0..40).collect::<Vec<u8>>();
    }
}
