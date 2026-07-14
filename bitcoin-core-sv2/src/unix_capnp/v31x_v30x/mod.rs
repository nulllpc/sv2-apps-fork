//! Shared implementation modules reused by Bitcoin Core v30.x and v31.x runtimes.

pub mod job_declaration_protocol;

// TDP shared monitors are reused via `#[path = "..."]` from v30x/v31x modules so they compile
// in each version-local `super::*` context; they are not exported from this module tree.
