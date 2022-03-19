# Changelog

## 0.2.0 (2022-03-19)

* Add `_rev` function to allow iteration / streaming in reverse topological order. These include:

    - `FnGraph::iter_rev`
    - `FnGraph::stream_rev`
    - `FnGraph::fold_rev_async`
    - `FnGraph::for_each_concurrent_rev`

## 0.1.0 (2021-12-11)

* Add `FnGraph` with ability to iterate sequentially or stream concurrently.