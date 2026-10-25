//! Component tests of the engine.
//!
//! [`state`] tests every session rule on the synchronous core with plain
//! inputs and outputs. [`actor`] covers what the core cannot see: waits,
//! permits, cancellation, and cleanup, against a fake transport made of bare
//! channels.

mod actor;
mod state;
