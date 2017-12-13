extern crate futures;

use futures::{Sink, Poll, StartSend};
use futures::Async::Ready;

struct MainSink<S> {
    sink: S, // TODO make this a Cell, so that this can be used from start_send and poll_complete, even they only require an immutable reference to a MainSink.
}

impl<S> MainSink<S> {
    pub fn sub_sink(&self) -> SubSink<S> {
        SubSink(&self)
    }
}

impl<S: Sink> MainSink<S> {
    pub fn into_inner(self) -> (S, Option<S::SinkError>) {
        unimplemented!()
    }

    // Invoked by the SubSink, unlike the Sink trait method, this does not require
    // a mutable reference.
    fn start_send(&self, item: S::SinkItem) -> StartSend<S::SinkItem, &S::SinkError> {
        unimplemented!()
    }

    // Invoked by the SubSink, unlike the Sink trait method, this does not require
    // a mutable reference.
    fn poll_complete(&self) -> Poll<(), &S::SinkError> {
        unimplemented!()
    }
}

struct SubSink<'s, S: 's>(&'s MainSink<S>);

impl<'s, S: Sink> Sink for SubSink<'s, S> {
    type SinkItem = S::SinkItem;
    type SinkError = &'s S::SinkError;

    /// Start sending on the underlying Sink.
    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        self.0.start_send(item)
    }

    /// Poll for completion on the underlying Sink.
    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.0.poll_complete()
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
