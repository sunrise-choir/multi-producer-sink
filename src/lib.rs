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

pub use unsync::*;
