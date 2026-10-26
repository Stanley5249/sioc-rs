//! Component tests of the manager.
//!
//! [`flow`], [`ack`], [`namespace`], [`engine`], and [`binary`] test every
//! protocol rule on the synchronous core: they send
//! [`ManagerInput`](crate::manager::state::ManagerInput)s and assert effects,
//! messages, and read decisions, with [`rig`] holding only builders. [`actor`]
//! covers what the core cannot see, the I/O of the actor, against a fake
//! engine.

mod ack;
mod actor;
mod binary;
mod engine;
mod flow;
mod namespace;
mod rig;
