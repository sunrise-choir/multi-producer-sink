//! Provides a multi-producer-sink, that allows multiple, independent handles to write to the same
//! underlying sink.
#![deny(missing_docs)]

extern crate futures_core;
extern crate futures_channel;
extern crate futures_sink;
extern crate futures_util;
extern crate indexmap;

#[cfg(test)]
extern crate atm_async_utils;
#[cfg(test)]
extern crate futures;

mod shared;
mod unsync;
mod sync;

use futures_core::Future;
use futures_sink::Sink;

pub use unsync::*;
pub use sync::*;

/// A multi producer sink (`MPS`). This is a cloneable handle to a single
/// sink of type `S`, and each handle can be used to write to the inner sink.
///
/// An error is signaled via the `Done`, the sink methods themselves only return `Err(())`. Upon
/// encountering an error, all handles are notified and they return `Err(())`. All further polling
/// will always yield `Err(None)` as well.
///
/// Unless an error occured, each of the handles must invoke `close` before being dropped. The
/// inner sink is closed when each of the handles has `close`d and emitted via the `Done`.
pub trait MPS<S: Sink>: Clone + Sink {
    /// A future that signals when the wrapped sink is done.
    ///
    /// Yields back the wrapped sink in an `Ok` when the last handle is closed or dropped.
    /// Emits the first error and the wrapped sink as an `Err` if the sink errors.
    type Done: Future<Item = S, Error = (S::SinkError, S)>;

    /// Create a new MPS from a sink and a `Done` to notify when it is done.
    fn mps(sink: S) -> (Self, Self::Done);
}
