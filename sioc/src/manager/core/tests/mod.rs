//! Component tests of the core. A [`rig::Rig`] plays the actor's part around
//! it, without a runtime, so each test reads as a sequence of inputs and the
//! effects they leave.

mod rig;

mod buffering;
mod engine;
mod namespaces;
mod server_packets;
