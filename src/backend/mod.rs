//! Handler backends: how a routed tool call reaches real work.
//!
//! Two backends ship with the crate:
//!
//! * [`NativeBackend`] — a bounded pool of reusable child processes speaking
//!   one framed JSON line per request. A registration may reserve a separate
//!   control session so cancellation-class tools never queue behind slow
//!   ordinary calls. Accepted work carries no internal deadline — a viable
//!   request runs to completion and only a real transport failure ends it.
//! * [`InProcessBackend`] — the embedding host's own handler, a trait-object
//!   call performed in the dispatcher's process.

mod in_process;
mod native;

pub use in_process::InProcessBackend;
pub use native::{NativeBackend, NativeSpec, REQUEST_PLACEHOLDER};
