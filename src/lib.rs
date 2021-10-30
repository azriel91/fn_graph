//! Batched function execution.
//!
//! This crate provides the ability to execute multiple functions in batches,
//! where the data required by each batch is guaranteed to not conflict
//! according to borrowing rules.
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
//! # Concepts
//!
//! ## Stages
//!
//! `fn_graph` provides the logic to stage functions such that there are no
//! conflicting data accesses within a stage of functions. Any would-be
//! conflicting data accesses are separated to an additional stage.
//!
//! The above scenario would contain two stages:
//!
//! | Stage | Functions  |
//! | :---: | ---------- |
//! |  `0`  | `f1`, `f2` |
//! |  `1`  | `f3`       |
//!
//! ## Dependencies
//!
//! Even if functions may not have conflicting data access, there may be logical
//! reasons to execute some functions before others.
//!
//! Assume in the set of functions, `f2` should logically occur after `f3`.
//!
//! | Function | Data                 |
//! | -------- | -------------------- |
//! | `f1`     | `&a,     &b`         |
//! | `f2`     | `&a,     &b, &mut c` |
//! | `f3`     | `&mut a, &b, &mut c` |
//!
//! The stages of functions that can execute are then:
//!
//! | Stage | Functions |
//! | :---: | --------- |
//! |  `0`  | `f1`      |
//! |  `1`  | `f3`      |
//! |  `2`  | `f2`      |
//!
//! # Notes
//!
//! The original implementation is from [`shred`]; `fn_graph` is an evolution of
//! `shred` with the following changes:
//!
//! * Updated API with ergonomic and flexibility trade-offs.
//!
//!     - Takes functions and closures as input instead of `System` impls.
//!
//!     - Parameters are detected from function signature instead of
//!       `SystemData` implementation, but with a limit of 7 parameters.
//!       *(manual `SystemData` implementation has arbitrary limit)*
//!
//!     - Return type is type parameterized instead of `()`.
//!
//! * Uses `async`, so does not group functions for concurrent / parallel
//!   execution.
//!
//! This crate builds upon [`resman`], which provides runtime managed data
//! borrowing.
//!
//! [`resman`]: https://github.com/azriel91/resman
//! [`shred`]: https://github.com/azriel91/shred
