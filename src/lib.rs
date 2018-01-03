//! This crate provides wrappers around sinks that allow multiple, independent
//! tasks to write to the same underlying sink.
//!
//! `MPS` uses reference counting: You create a new `MPS`, which consumes a sink.
//! This `MPS` can then be cheaply cloned, and each one can be used to write to
//! the same, underlying sink.
//!
//! `OwnerMPS` consumes a sink, and has a method to obtain `Handles` to it. These
//! `Handles` can be used to write to the owner's sink, but they can not outlive
//! it.
#![deny(missing_docs)]

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
mod shared;

pub use rc_mps::*;
pub use borrow_mps::*;
