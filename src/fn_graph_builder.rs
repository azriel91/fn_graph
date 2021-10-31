use std::mem::MaybeUninit;

use daggy::WouldCycle;

use crate::{Edge, EdgeId, FnGraph, FnId};

/// Builder for a [`FnGraph`].
#[derive(Debug)]
pub struct FnGraphBuilder<F> {
    /// Directed acyclic graph of functions.
    fn_graph: FnGraph<F>,
}

impl<F> FnGraphBuilder<F> {
    /// Returns a new `FnGraphBuilder`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a function to the graph.
    ///
    /// The returned function ID is used to specify dependencies between
    /// functions through the [`add_edge`] method.
    ///
    /// [`add_edge`]: Self::add_edge
    pub fn add_function(&mut self, function_spec: F) -> FnId {
        self.fn_graph.add_node(function_spec)
    }

    /// Adds multiple functions to the graph.
    ///
    /// The returned function IDs are used to specify dependencies between
    /// functions through the [`add_edge`] / [`add_edges`] method.
    ///
    /// [`add_edge`]: Self::add_edge
    /// [`add_edges`]: Self::add_edges
    pub fn add_functions<const N: usize>(&mut self, fn_graph: [F; N]) -> [FnId; N] {
        // Create an uninitialized array of `MaybeUninit`. The `assume_init` is safe
        // because the type we are claiming to have initialized here is a bunch of
        // `MaybeUninit`s, which do not require initialization.
        //
        // https://doc.rust-lang.org/stable/std/mem/union.MaybeUninit.html#initializing-an-array-element-by-element
        //
        // Switch this to `MaybeUninit::uninit_array` once it is stable.
        let mut fn_ids: [MaybeUninit<FnId>; N] = unsafe { MaybeUninit::uninit().assume_init() };

        IntoIterator::into_iter(fn_graph)
            .map(|function_spec| self.add_function(function_spec))
            .zip(fn_ids.iter_mut())
            .for_each(|(function_rt_id, function_rt_id_mem)| {
                function_rt_id_mem.write(function_rt_id);
            });

        // Everything is initialized. Transmute the array to the initialized type.
        // Unfortunately we cannot use this, see the following issues:
        //
        // * <https://github.com/rust-lang/rust/issues/61956>
        // * <https://github.com/rust-lang/rust/issues/80908>
        //
        // let fn_ids = unsafe { mem::transmute::<_, [NodeIndex<FnId>; N]>(fn_ids) };

        #[allow(clippy::let_and_return)] // for clarity with `unsafe`
        let fn_ids = {
            let ptr = &mut fn_ids as *mut _ as *mut [FnId; N];
            let array = unsafe { ptr.read() };

            // We don't have to `mem::forget` the original because `FnId` is `Copy`.
            // mem::forget(fn_ids);

            array
        };

        fn_ids
    }

    /// Adds an edge from one function to another.
    ///
    /// This differs from [`petgraph`'s `add_edge`] in that this only allows one
    /// edge between two functions. When this function is called multiple times
    /// with the same functions, only the last call's edge will be retained.
    ///
    /// [`petgraph`'s `add_edge`]: daggy::petgraph::data::Build::add_edge
    pub fn add_edge(
        &mut self,
        function_from: FnId,
        function_to: FnId,
    ) -> Result<EdgeId, WouldCycle<Edge>> {
        // Use `update_edge` instead of `add_edge` to avoid duplicate edges from one
        // function to the other.
        self.fn_graph
            .update_edge(function_from, function_to, Edge::Logic)
    }

    /// Adds edges between functions.
    pub fn add_edges<const N: usize>(
        &mut self,
        edges: [(FnId, FnId); N],
    ) -> Result<[EdgeId; N], WouldCycle<Edge>> {
        // Create an uninitialized array of `MaybeUninit`. The `assume_init` is safe
        // because the type we are claiming to have initialized here is a bunch of
        // `MaybeUninit`s, which do not require initialization.
        //
        // https://doc.rust-lang.org/stable/std/mem/union.MaybeUninit.html#initializing-an-array-element-by-element
        //
        // Switch this to `MaybeUninit::uninit_array` once it is stable.
        let mut edge_ids: [MaybeUninit<EdgeId>; N] = unsafe { MaybeUninit::uninit().assume_init() };

        IntoIterator::into_iter(edges)
            .zip(edge_ids.iter_mut())
            .try_for_each(|((function_from, function_to), edge_index_mem)| {
                self.add_edge(function_from, function_to).map(|edge_index| {
                    edge_index_mem.write(edge_index);
                })
            })?;

        // Everything is initialized. Transmute the array to the initialized type.
        // Unfortunately we cannot use this, see the following issues:
        //
        // * <https://github.com/rust-lang/rust/issues/61956>
        // * <https://github.com/rust-lang/rust/issues/80908>
        //
        // let edge_ids = unsafe { mem::transmute::<_, [EdgeId; N]>(edge_ids) };

        #[allow(clippy::let_and_return)] // for clarity with `unsafe`
        let edge_ids = {
            let ptr = &mut edge_ids as *mut _ as *mut [EdgeId; N];
            let array = unsafe { ptr.read() };

            // We don't have to `mem::forget` the original because `EdgeId` is `Copy`.
            // mem::forget(edge_ids);

            array
        };

        Ok(edge_ids)
    }

    /// Builds and returns the [`FnGraph`].
    pub fn build(self) -> FnGraph<F> {
        self.fn_graph
    }
}

impl<F> Default for FnGraphBuilder<F> {
    fn default() -> Self {
        Self {
            fn_graph: FnGraph::default(),
        }
    }
}
