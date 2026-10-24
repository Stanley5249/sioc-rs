//! Component tests of the manager: [`actor`] drives a whole manager against a
//! fake engine, and [`state`] drives the synchronous core with plain
//! `#[test]`s through [`rig::Rig`], which plays the actor's part.

mod actor;
mod rig;
mod state;
