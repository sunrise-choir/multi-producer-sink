#[macro_use]
extern crate futures;

#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
extern crate rand;
#[cfg(test)]
extern crate void;
#[cfg(test)]
extern crate atm_async_utils;

use std::collections::vec_deque::VecDeque;
use std::cell::{Cell, UnsafeCell};
use std::mem;
use std::ptr;

use futures::{Sink, Poll, StartSend, AsyncSink, Async, Future, Stream};
use futures::Async::{Ready, NotReady};
use futures::stream::Fuse;
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
    // Tasks blocked on `start_send`, with the id of their SubSink.
    send_tasks: VecDeque<(Task, usize)>,
    // Tasks blocked on `poll_complete`, with the id of their SubSink.
    complete_tasks: VecDeque<(Task, usize)>,
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
        println!("impl start_send, id: {}, item: {:?}", id, item);
        println!("send_tasks: {:?}", self.send_tasks);
        if self.did_error {
            // After the first error, stop normal operation.
            Err(&self.error)
        } else {
            println!("send: not errored");
            // We haven't errored yet, so check whether we are blocking on anything.
            if self.complete_tasks.is_empty() {
                println!("send: no complete tasks");
                // Due to multiple mutable references, we can not simply match on
                // `self.send_tasks.get(0)`, so we do a little dance to appease
                // the compiler.
                let is_empty;
                let front_id;
                {
                    let front = self.send_tasks.get(0);
                    is_empty = front.is_none();
                    front_id = front.map_or(0, |pair| pair.1);
                }

                if is_empty {
                    println!("send: no send tasks");
                    // Underlying sink is not blocking at all.
                    match self.sink.start_send(item) {
                        Ok(AsyncSink::Ready) => {
                            println!("send: sent to inner");
                            return Ok(AsyncSink::Ready);
                        }
                        Ok(AsyncSink::NotReady(item)) => {
                            println!("send: blocking on inner send, own id: {}", id);
                            self.send_tasks.push_back((current(), id));
                            return Ok(AsyncSink::NotReady(item));
                        }
                        Err(e) => {
                            self.set_error(e);
                            return Err(&self.error);
                        }
                    }
                } else {
                    println!("send: blocking on send");
                    // Underlying sink is blocking on poll_send.
                    if id == front_id {
                        println!("send: blocking on this, own id: {}", id);
                        // The underlying sink woke the task, so we can try again.
                        match self.sink.start_send(item) {
                            Ok(AsyncSink::Ready) => {
                                let _ = self.send_tasks.pop_front();
                                println!("popped from front, own: {}, front: {}", id, front_id);
                                // notify the next task
                                let front = self.send_tasks.get(0);
                                front.map(|entry| entry.0.notify());
                                return Ok(AsyncSink::Ready);
                            }
                            Ok(AsyncSink::NotReady(item)) => {
                                println!("blocked for {}", id);
                                return Ok(AsyncSink::NotReady(item));
                            }
                            Err(e) => {
                                let _ = self.send_tasks.pop_front();
                                self.set_error(e);
                                return Err(&self.error);
                            }
                        }
                    } else {
                        println!("send: blocking on other, own id: {}", id);
                        // Blocking on another send task
                        self.send_tasks.push_back((current(), id));
                        return Ok(AsyncSink::NotReady(item));
                    }
                }
            } else {
                println!("send: blocking on complete");
                // The underlying sink is blocking on poll_complete.
                self.send_tasks.push_back((current(), id));
                return Ok(AsyncSink::NotReady(item));
            }
        }
    }

    // Invoked by the SubSink, unlike the Sink trait method, this does not require
    // a mutable reference.
    fn poll_complete(&mut self, id: usize) -> Poll<(), &S::SinkError> {
        println!("impl poll_complete, id: {}", id);
        if self.did_error {
            Err(&self.error)
        } else {
            println!("complete: not errored");
            // We haven't errored yet, so check whether we are blocking on anything.
            if self.send_tasks.is_empty() {
                println!("complete: no sends");
                // Due to multiple mutable references, we can not simply match on
                // `self.send_tasks.get(0)`, so we do a little dance to appease
                // the compiler.
                let is_empty;
                let front_id;
                {
                    let front = self.complete_tasks.get(0);
                    is_empty = front.is_none();
                    front_id = front.map_or(0, |pair| pair.1);
                }

                if is_empty {
                    println!("complete: not blocking at all");
                    // Underlying sink is not blocking at all.
                    match self.sink.poll_complete() {
                        Ok(Async::Ready(_)) => {
                            return Ok(Async::Ready(()));
                        }
                        Ok(Async::NotReady) => {
                            self.complete_tasks.push_back((current(), id));
                            return Ok(Async::NotReady);
                        }
                        Err(e) => {
                            self.set_error(e);
                            return Err(&self.error);
                        }
                    }
                } else {
                    // Underlying sink is blocking on poll_complete.
                    if id == front_id {
                        // The underlying sink woke the task, so we can try again.
                        match self.sink.poll_complete() {
                            Ok(Async::Ready(_)) => {
                                let _ = self.complete_tasks.pop_front();
                                return Ok(Async::Ready(()));
                            }
                            Ok(Async::NotReady) => {
                                return Ok(Async::NotReady);
                            }
                            Err(e) => {
                                let _ = self.complete_tasks.pop_front();
                                self.set_error(e);
                                return Err(&self.error);
                            }
                        }
                    } else {
                        // Blocking on another complete task
                        self.complete_tasks.push_back((current(), front_id));
                        return Ok(Async::NotReady);
                    }
                }
            } else {
                // The underlying sink is blocking on start_send.
                self.complete_tasks.push_back((current(), id));
                return Ok(Async::NotReady);
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

// TODO move to utils
/// Future which closes a sink.
pub struct Close<S: Sink> {
    sink: Option<S>,
}

impl<S: Sink> Close<S> {
    pub fn new(s: S) -> Close<S> {
        Close { sink: Some(s) }
    }

    /// Get a shared reference to the inner sink.
    pub fn get_ref(&self) -> &S {
        self.sink
            .as_ref()
            .take()
            .expect("Attempted Close::get_ref after completion")
    }

    /// Get a mutable reference to the inner sink.
    pub fn get_mut(&mut self) -> &mut S {
        self.sink
            .as_mut()
            .take()
            .expect("Attempted Close::get_mut after completion")
    }
}

impl<S: Sink> Future for Close<S> {
    type Item = S;
    type Error = S::SinkError;

    fn poll(&mut self) -> Poll<S, S::SinkError> {
        let mut s = self.sink
            .take()
            .expect("Attempted to poll Close after completion");

        match s.close() {
            Ok(Async::Ready(_)) => {
                return Ok(Async::Ready(s));
            }
            Ok(Async::NotReady) => {
                self.sink = Some(s);
                return Ok(Async::NotReady);
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
}

// TODO move to utils
/// Future which sends all items from a Stream into a Sink. Unlike the tokio SendAll Future, this
/// does not close the sink (it does flush though).
pub struct SendAll<T, U: Stream> {
    sink: Option<T>,
    stream: Option<Fuse<U>>,
    buffered: Option<U::Item>,
}

impl<T, U> SendAll<T, U>
    where T: Sink,
          U: Stream<Item = T::SinkItem>,
          T::SinkError: From<U::Error>
{
    pub fn new(sink: T, stream: U) -> SendAll<T, U> {
        SendAll {
            sink: Some(sink),
            stream: Some(stream.fuse()),
            buffered: None,
        }
    }

    fn sink_mut(&mut self) -> &mut T {
        self.sink
            .as_mut()
            .take()
            .expect("Attempted to poll SendAll after completion")
    }

    fn stream_mut(&mut self) -> &mut Fuse<U> {
        self.stream
            .as_mut()
            .take()
            .expect("Attempted to poll SendAll after completion")
    }

    fn take_result(&mut self) -> (T, U) {
        let sink = self.sink
            .take()
            .expect("Attempted to poll Forward after completion");
        let fuse = self.stream
            .take()
            .expect("Attempted to poll Forward after completion");
        (sink, fuse.into_inner())
    }

    fn try_start_send(&mut self, item: U::Item) -> Poll<(), T::SinkError> {
        debug_assert!(self.buffered.is_none());
        if let AsyncSink::NotReady(item) = self.sink_mut().start_send(item)? {
            self.buffered = Some(item);
            return Ok(Async::NotReady);
        }
        Ok(Async::Ready(()))
    }
}

impl<T, U> Future for SendAll<T, U>
    where T: Sink,
          U: Stream<Item = T::SinkItem>,
          T::SinkError: From<U::Error>
{
    type Item = (T, U);
    type Error = T::SinkError;

    fn poll(&mut self) -> Poll<(T, U), T::SinkError> {
        // If we've got an item buffered already, we need to write it to the
        // sink before we can do anything else
        if let Some(item) = self.buffered.take() {
            try_ready!(self.try_start_send(item))
        }

        loop {
            match self.stream_mut().poll()? {
                Async::Ready(Some(item)) => try_ready!(self.try_start_send(item)),
                Async::Ready(None) => {
                    return Ok(Async::Ready(self.take_result()));
                }
                Async::NotReady => {
                    try_ready!(self.sink_mut().poll_complete());
                    return Ok(Async::NotReady);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::{Future, Stream};
    use futures::future::poll_fn;
    use futures::stream::iter_ok;

    use quickcheck::{QuickCheck, StdGen, Gen, Arbitrary};
    use rand;
    use void::Void;
    use rand::Rng;
    use atm_async_utils::test_channel::*;
    use atm_async_utils::test_sink::*;

    // #[test] // TODO enable this test
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
        let mut quickcheck = QuickCheck::new().gen(rng).tests(1000); // TODO increase number of runs
        quickcheck.quickcheck(success as fn(usize) -> bool);
    }

    fn success(buf_size: usize) -> bool {
        println!("");
        // println!("test with buffer of size {}", buf_size + 1);
        // let (sender, receiver) = test_channel::<u8, Void, Void>(buf_size + 1);
        println!("test with buffer of size {}", 1);
        let (sender, receiver) = test_channel::<u8, Void, Void>(1);

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
            .and_then(|(_, _, _, (s4, _))| {
                          println!("reached close-closure");
                          Close::new(s4).map_err(|x| *x)
                      });

        // TODO test flushing

        let (mut received, _) = receiver.collect().join(sending).wait().unwrap();
        println!("{:?}", received);
        received.sort();

        return received == (0..40).collect::<Vec<u8>>();
    }
}
