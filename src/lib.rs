//! Dynamically managed function graph execution.
//!
//! This crate provides a `FnGraph`, where consumers can register a list of
//! functions and their interdependencies. The graph can then return a stream of
//! functions to iterate over either sequentially or concurrently. Any data
//! dependencies required by the functions are guaranteed to not conflict
//! according to borrowing rules.
//!
//! There is additional flexibility that the type of functions is not limited to
//! closures and functions, but any type that implements the [`FnRes`] and
//! [`FnMeta`] traits.
//!
//! # Usage
//!
//! Add the following to `Cargo.toml`
//!
//! ```toml
//! fn_graph = "0.5.4"
//!
//! # Integrate with `fn_meta` and/or `resman`
//! fn_graph = { version = "0.5.4", features = ["fn_meta"] }
//! fn_graph = { version = "0.5.4", features = ["resman"] }
//! fn_graph = { version = "0.5.4", features = ["fn_meta", "resman"] }
//! ```
//!
//! # Rationale
//!
//! Support there are three tasks, each represented by a function. Each function
//! needs different data:
//!
//! | Function | Data                 |
//! | -------- | -------------------- |
//! | `f1`     | `&a,     &b`         |
//! | `f2`     | `&a,     &b, &mut c` |
//! | `f3`     | `&mut a, &b, &mut c` |
//!
//! When scheduling parallel execution, it is valid to execute `f1` and `f2` in
//! parallel, since data `a` and `b` are accessed immutably, and `c` is
//! exclusively accessed by `b`. `f3` cannot be executed in parallel with `f1`
//! or `f2` as it requires exclusive access to both `a` and `c`.
//!
//! For a small number of functions and data, manually writing code to
//! schedule function execution is manageable. However, as the number of
//! functions and data increases, so does its complexity, and it is desirable
//! for this boilerplate to be managed by code.
//!
//! # Notes
//!
//! The concept of a runtime managed data-dependency task graph is from
//! [`shred`]; `fn_graph`'s implementation has the following differences:
//!
//! * Different API ergonomics and flexibility trade-offs.
//!
//!     - Takes functions and closures as input instead of `System` impls.
//!
//!     - Parameters are detected from function signature instead of
//!       `SystemData` implementation, but with a limit of 8 parameters.
//!       *(manual `SystemData` implementation has arbitrary limit)*
//!
//!     - Return type is type parameterized instead of `()`.
//!
//! * Instead of grouping functions by stages to manage data access conflicts,
//!   `fn_graph` keeps a dependency graph of logic and data dependencies, and
//!   executes functions when the preceding functions are complete.
//!
//!     This allows for slightly less waiting time for subsequent functions with
//!     data dependencies, as each may begin once its predecessors finish,
//!     whereas a staged approach may contain other functions that are still
//!     executing that prevent functions in the next stage from beginning
//!     execution.
//!
//! ## See Also
//!
//! * [`fn_meta`]: Returns metadata about a function at runtime.
//! * [`resman`]: Runtime managed resource borrowing.
//! * [`shred`]: Shared resource dispatcher.
//!
//! [`fn_meta`]: https://github.com/azriel91/fn_meta
//! [`resman`]: https://github.com/azriel91/resman
//! [`shred`]: https://github.com/amethyst/shred
//!
//! [`FnMeta`]: https://docs.rs/fn_meta/latest/fn_meta/trait.FnMeta.html
//! [`FnRes`]: https://docs.rs/resman/latest/resman/trait.FnRes.html

pub use daggy::{self, WouldCycle};

#[cfg(feature = "resman")]
pub use resman::{self, Resources};

#[cfg(feature = "fn_meta")]
pub use fn_meta::{FnMeta, FnMetaExt, FnMetadata, FnMetadataExt};

pub use crate::{
    data_access::{DataAccess, DataAccessDyn, R, W},
    edge::Edge,
    edge_counts::EdgeCounts,
    edge_id::EdgeId,
    fn_graph::FnGraph,
    fn_graph_builder::FnGraphBuilder,
    fn_id::FnId,
    fn_id_inner::FnIdInner,
    rank::Rank,
    type_ids::TypeIds,
};

#[cfg(feature = "resman")]
pub use crate::data_access::DataBorrow;

mod data_access;
mod edge;
mod edge_counts;
mod edge_id;
mod fn_graph;
mod fn_graph_builder;
mod fn_id;
mod fn_id_inner;
mod rank;
mod type_ids;

#[cfg(feature = "async")]
pub use crate::{fn_ref::FnRef, fn_ref_mut::FnRefMut};

#[cfg(feature = "async")]
mod fn_ref;
#[cfg(feature = "async")]
mod fn_ref_mut;
