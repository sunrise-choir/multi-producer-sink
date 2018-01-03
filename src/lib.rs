//! This crate provides wrappers around sinks that allow multiple, independent
//! tasks to write to the same underlying sink.
#![deny(missing_docs)]

// TODO update readme/cargo.toml description
// TODO provide MPS which uses lifetimes rather than reference counting

// TODO clean up Void import and git imports in general
extern crate futures;
extern crate ordermap;

#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
extern crate rand;
#[cfg(test)]
extern crate void;
#[cfg(test)]
extern crate atm_async_utils;

mod rc_mps;
mod borrow_mps;
mod id_task;

pub use rc_mps::*;
pub use borrow_mps::*;
