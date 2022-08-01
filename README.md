# 🧬 fn_graph

[![Crates.io](https://img.shields.io/crates/v/fn_graph.svg)](https://crates.io/crates/fn_graph)
[![docs.rs](https://img.shields.io/docsrs/fn_graph)](https://docs.rs/fn_graph)
[![CI](https://github.com/azriel91/fn_graph/workflows/CI/badge.svg)](https://github.com/azriel91/fn_graph/actions/workflows/ci.yml)
[![Coverage Status](https://codecov.io/gh/azriel91/fn_graph/branch/main/graph/badge.svg)](https://codecov.io/gh/azriel91/fn_graph)

Dynamically managed function graph execution.

This crate provides a `FnGraph`, where consumers can register a list of
functions and their interdependencies. The graph can then return a stream of
functions to iterate over either sequentially or concurrently. Any data
dependencies required by the functions are guaranteed to not conflict
according to borrowing rules.

There is additional flexibility that the type of functions is not limited to
closures and functions, but any type that implements the [`FnRes`] and
[`FnMeta`] traits.

# Usage

Add the following to `Cargo.toml`

```toml
fn_graph = "0.5.3"

# Integrate with `fn_meta` and/or `resman`
fn_graph = { version = "0.5.3", features = ["fn_meta"] }
fn_graph = { version = "0.5.3", features = ["resman"] }
fn_graph = { version = "0.5.3", features = ["fn_meta", "resman"] }
```

# Rationale

Support there are three tasks, each represented by a function. Each function
needs different data:

| Function | Data                 |
| -------- | -------------------- |
| `f1`     | `&a,     &b`         |
| `f2`     | `&a,     &b, &mut c` |
| `f3`     | `&mut a, &b, &mut c` |

When scheduling parallel execution, it is valid to execute `f1` and `f2` in
parallel, since data `a` and `b` are accessed immutably, and `c` is
exclusively accessed by `b`. `f3` cannot be executed in parallel with `f1`
or `f2` as it requires exclusive access to both `a` and `c`.

For a small number of functions and data, manually writing code to
schedule function execution is manageable. However, as the number of
functions and data increases, so does its complexity, and it is desirable
for this boilerplate to be managed by code.

# Notes

The concept of a runtime managed data-dependency task graph is from
[`shred`]; `fn_graph`'s implementation has the following differences:

* Different API ergonomics and flexibility trade-offs.

    - Takes functions and closures as input instead of `System` impls.

    - Parameters are detected from function signature instead of
      `SystemData` implementation, but with a limit of 8 parameters.
      *(manual `SystemData` implementation has arbitrary limit)*

    - Return type is type parameterized instead of `()`.

* Instead of grouping functions by stages to manage data access conflicts,
  `fn_graph` keeps a dependency graph of logic and data dependencies, and
  executes functions when the preceding functions are complete.

    This allows for slightly less waiting time for subsequent functions with
    data dependencies, as each may begin once its predecessors finish,
    whereas a staged approach may contain other functions that are still
    executing that prevent functions in the next stage from beginning
    execution.

## See Also

* [`fn_meta`]: Returns metadata about a function at runtime.
* [`resman`]: Runtime managed resource borrowing.
* [`shred`]: Shared resource dispatcher.

[`fn_meta`]: https://github.com/azriel91/fn_meta
[`resman`]: https://github.com/azriel91/resman
[`shred`]: https://github.com/amethyst/shred

## License

Licensed under either of

* Apache License, Version 2.0, ([LICENSE-APACHE] or <https://www.apache.org/licenses/LICENSE-2.0>)
* MIT license ([LICENSE-MIT] or <https://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.

[LICENSE-APACHE]: LICENSE-APACHE
[LICENSE-MIT]: LICENSE-MIT

[`FnMeta`]: https://docs.rs/fn_meta/latest/fn_meta/trait.FnMeta.html
[`FnRes`]: https://docs.rs/resman/latest/resman/trait.FnRes.html
