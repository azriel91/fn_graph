# Changelog

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
