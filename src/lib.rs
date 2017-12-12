extern crate futures;

use futures::{Sink, Poll, StartSend};

struct MainSink<S> {
    sink: S,
}

impl<S> MainSink<S> {
    pub fn sub_sink(&self) -> SubSink<S> {
        unimplemented!()
    }
}

impl<S: Sink> MainSink<S> {
    pub fn into_inner(self) -> (S, Option<S::SinkError>) {
        unimplemented!()
    }
}

struct SubSink<'s, S: 's> {
    main: &'s MainSink<S>,
}

impl<'s, S: Sink> Sink for SubSink<'s, S> {
    type SinkItem = S::SinkItem;
    type SinkError = &'s S::SinkError;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        unimplemented!()
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        unimplemented!()
    }

    fn close(&mut self) -> Poll<(), Self::SinkError> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
