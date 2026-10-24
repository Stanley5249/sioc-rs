//! Component tests of the manager.
//!
//! [`state`] and [`binary`] test every protocol rule on the synchronous core:
//! they send [`ManagerInput`](crate::manager::state::ManagerInput)s and assert
//! effects, messages, and read decisions, with [`rig`] holding only builders.
//! [`actor`] covers what the core cannot see, the I/O of the actor, against a
//! fake engine.

mod actor;
mod binary;
mod namespace;
mod rig;
mod state;
