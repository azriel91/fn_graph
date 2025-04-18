# Changelog

## 0.18.0 (2025-04-18)

* Update `daggy` from `0.8.1` to `0.9.0`.


## 0.17.0 (2025-04-07)

* Update crate rust edition to 2024.
* Update `resman` from `0.18.0` to `0.19.0`.


## 0.16.0 (2025-02-22)

* Switch from `daggy2` to `daggy`.


## 0.15.0 (2025-01-12)

* Update `resman` from `0.17.0` to `0.18.0`.


## 0.14.0 (2025-01-11)

* Update dependency versions.
* Switch from `daggy` to `daggy2`.


## 0.13.3 (2024-08-31)

* Update dependency versions.


## 0.13.2 (2024-03-10)

* Add `GraphInfo::new`.
* Make `GraphInfo` fields public.


## 0.13.1 (2024-03-10)

* `GraphInfo`: Use `Reversed` instead of storing separate `graph_structure_rev`.


## 0.13.0 (2024-03-05)

* ***Breaking:*** Change `FnGraphBuilder::add_edge(s)` to `FnGraphBuilder::add_{logic,contains}_edge(s)`.
* Add serializable `GraphInfo` struct gated behind `"graph_info"` feature.
* Add `R::new` and `W::new` when `"resman"` feature is disabled.


## 0.12.0 (2024-01-04)

* Add `StreamOpts::interrupted_next_item_include` to toggle whether interrupted item is streamed.
* Separate `FnGraph::stream` and `FnGraph::stream_with`, from `FnGraph::stream_interruptible` and `FnGraph::stream_with_interruptible`.


## 0.11.0 (2024-01-03)

* Fix `StreamOutcome` `fn_ids_processed` and `fn_ids_not_processed` values.
* Remove `StreamProgress` and `StreamProgressState`.
* Update `interruptible` to `0.2.1`.


## 0.10.0 (2023-12-28)

* Replace `StreamProgress::{empty, with_capacity}` with `StreamProgress::new`.
* Actually return values in `StreamOutcome::fn_ids_not_processed`.


## 0.9.1 (2023-12-14)

* Add `StreamOutcome::replace_with`.


## 0.9.0 (2023-11-28)

* Add `interruptible` support with `features = ["interruptible"]`.
* Add `StreamOpts` to specify streaming options.
* Add `FnGraph::stream_with`.
* Add `FnGraph::fold_async_with`.
* Add `FnGraph::fold_async_mut_with`.
* Add `FnGraph::for_each_concurrent_with`.
* Add `FnGraph::for_each_concurrent_mut_with`.
* Add `FnGraph::try_fold_async_with`.
* Add `FnGraph::try_fold_async_mut_with`.
* Add `FnGraph::try_for_each_concurrent_with`.
* Add `FnGraph::try_for_each_concurrent_control_with`.
* Add `FnGraph::try_for_each_concurrent_mut_with`.
* Add `FnGraph::try_for_each_concurrent_control_mut_with`.


## 0.8.6 (2023-09-23)

* Update dependency versions.


## 0.8.5 (2023-09-16)

* Update dependency versions.
* Update coverage attribute due to `cargo-llvm-cov` upgrade.


## 0.8.4 (2023-07-01)

* Add `FnGraph::try_for_each_concurrent_control`.
* Add `FnGraph::try_for_each_concurrent_control_rev`.
* Add `FnGraph::try_for_each_concurrent_control_mut`.
* Add `FnGraph::try_for_each_concurrent_control_mut_rev`.


## 0.8.3 (2023-05-27)

* Implement `PartialEq for FnGraph`.
* Update dependency versions.


## 0.8.2 (2023-04-06)

* Update dependency versions.


## 0.8.1 (2023-01-18)

* Fixed streaming not returning when graph is empty.


## 0.8.0 (2023-01-01)

* Add separate `*for_each_concurrent*` methods for immutable fn refs.
* No longer require future to be boxed for `*for_each_concurrent*` methods.


## 0.7.0 (2022-12-31)

* Implement `FnGraph::try_for_each_concurrent`.
* Implement `FnGraph::try_for_each_concurrent_rev`.


## 0.6.0 (2022-12-26)

* Update dependency versions.


## 0.5.4 (2022-08-02)

* Implement `DataAccess`, `DataAccessDyn` for `&()`.
* Update `fn_meta` to `0.7.3`.


## 0.5.3 (2022-08-02)

* Implement `DataAccess`, `DataAccessDyn` for `()`.
* Update dependency versions.


## 0.5.2 (2022-07-06)

* Update dependency versions.


## 0.5.1 (2022-06-05)

* Add `Self: Sized` bound to `DataAccess` trait methods for object safety.


## 0.5.0 (2022-05-26)

* Update `fn_meta` to `0.7.0`.
* Add `resman` integration through the `"resman"` feature.


## 0.4.0 (2022-05-22)

* Update `fn_meta` to `0.5.0`.


## 0.3.0 (2022-05-15)

* Add `DataAccess` trait, and `R` and `W` types.
* Feature gate `fn_meta` crate.


## 0.2.0 (2022-03-19)

* Add `_rev` function to allow iteration / streaming in reverse topological order. These include:

    - `FnGraph::iter_rev`
    - `FnGraph::stream_rev`
    - `FnGraph::fold_rev_async`
    - `FnGraph::for_each_concurrent_rev`


## 0.1.0 (2021-12-11)

* Add `FnGraph` with ability to iterate sequentially or stream concurrently.
