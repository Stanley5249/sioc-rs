//! Component tests of the manager.
//!
//! [`flow`], [`ack`], [`namespace`], [`engine`], and [`binary`] test every
//! protocol rule on the synchronous core: they call its transitions and assert
//! effects and messages, with [`rig`] holding only builders. [`driver`]
//! covers what the core cannot see, the I/O of the driver and the handles,
//! against a fake engine.

mod ack;
mod binary;
mod driver;
mod engine;
mod flow;
mod namespace;
mod ownership;
mod rig;
