use std::mem::MaybeUninit;

use daggy::{petgraph::algo::toposort, Dag, WouldCycle};
use resman::FnRes;

use crate::{Edge, EdgeId, FnGraph, FnId, FnIdInner};

use self::{data_edge_augmenter::DataEdgeAugmenter, rank_calc::RankCalc};

mod data_edge_augmenter;
mod rank_calc;

/// Builder for a [`FnGraph`].
#[derive(Debug)]
pub struct FnGraphBuilder<F> {
    /// Directed acyclic graph of functions.
    graph: Dag<F, Edge, FnIdInner>,
}

impl<F> FnGraphBuilder<F>
where
    F: FnRes,
{
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
    pub fn add_fn(&mut self, f: F) -> FnId {
        self.graph.add_node(f)
    }

    /// Adds multiple functions to the graph.
    ///
    /// The returned function IDs are used to specify dependencies between
    /// functions through the [`add_edge`] / [`add_edges`] method.
    ///
    /// [`add_edge`]: Self::add_edge
    /// [`add_edges`]: Self::add_edges
    pub fn add_fns<const N: usize>(&mut self, fns: [F; N]) -> [FnId; N] {
        // Create an uninitialized array of `MaybeUninit`. The `assume_init` is safe
        // because the type we are claiming to have initialized here is a bunch of
        // `MaybeUninit`s, which do not require initialization.
        //
        // https://doc.rust-lang.org/stable/std/mem/union.MaybeUninit.html#initializing-an-array-element-by-element
        //
        // Switch this to `MaybeUninit::uninit_array` once it is stable.
        let mut fn_ids: [MaybeUninit<FnId>; N] = unsafe { MaybeUninit::uninit().assume_init() };

        IntoIterator::into_iter(fns)
            .map(|f| self.add_fn(f))
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
        self.graph
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
        let Self { mut graph } = self;
        let ranks = RankCalc::calc(&graph);
        DataEdgeAugmenter::augment(&mut graph, &ranks);
        let toposort = toposort(&graph, None).expect("Graph should not contain any cycles");

        FnGraph {
            graph,
            ranks,
            toposort,
        }
    }
}

impl<F> Default for FnGraphBuilder<F> {
    fn default() -> Self {
        Self {
            graph: Dag::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use daggy::WouldCycle;
    use resman::IntoFnRes;

    use super::FnGraphBuilder;
    use crate::{Edge, Rank};

    #[test]
    fn add_fn_with_differing_fns() {
        let mut fn_graph_builder = FnGraphBuilder::new();
        fn_graph_builder.add_fn((|| {}).into_fn_res());
        fn_graph_builder.add_fn((|_: &usize| {}).into_fn_res());
        fn_graph_builder.add_fn((|_: &mut usize, _: &mut u32| {}).into_fn_res());

        let fn_graph = fn_graph_builder.build();

        assert_eq!(&[Rank(0), Rank(0), Rank(0)], fn_graph.ranks.as_slice());
    }

    #[test]
    fn add_fns() {
        let mut fn_graph_builder = FnGraphBuilder::new();
        fn_graph_builder.add_fns([
            (|| {}).into_fn_res(),
            (|_: &usize| {}).into_fn_res(),
            (|_: &mut usize, _: &mut u32| {}).into_fn_res(),
        ]);

        let fn_graph = fn_graph_builder.build();

        assert_eq!(&[Rank(0), Rank(0), Rank(0)], fn_graph.ranks.as_slice());
    }

    #[test]
    fn add_edge() -> Result<(), WouldCycle<Edge>> {
        let mut fn_graph_builder = FnGraphBuilder::new();
        let fn_a = fn_graph_builder.add_fn((|| {}).into_fn_res());
        let fn_b = fn_graph_builder.add_fn((|_: &usize| {}).into_fn_res());
        let fn_c = fn_graph_builder.add_fn((|_: &mut usize, _: &mut u32| {}).into_fn_res());
        let _fn_d = fn_graph_builder.add_fn((|_: &usize, _: &u32| {}).into_fn_res());
        fn_graph_builder.add_edge(fn_a, fn_b)?;
        fn_graph_builder.add_edge(fn_b, fn_c)?;

        let fn_graph = fn_graph_builder.build();

        assert_eq!(
            &[Rank(0), Rank(1), Rank(2), Rank(0)],
            fn_graph.ranks.as_slice()
        );
        Ok(())
    }

    #[test]
    fn add_edges() -> Result<(), WouldCycle<Edge>> {
        let mut fn_graph_builder = FnGraphBuilder::new();
        let fn_a = fn_graph_builder.add_fn((|| {}).into_fn_res());
        let fn_b = fn_graph_builder.add_fn((|_: &usize| {}).into_fn_res());
        let fn_c = fn_graph_builder.add_fn((|_: &mut usize, _: &mut u32| {}).into_fn_res());
        let _fn_d = fn_graph_builder.add_fn((|_: &usize, _: &u32| {}).into_fn_res());
        fn_graph_builder.add_edges([(fn_a, fn_b), (fn_b, fn_c)])?;

        let fn_graph = fn_graph_builder.build();

        assert_eq!(
            &[Rank(0), Rank(1), Rank(2), Rank(0)],
            fn_graph.ranks.as_slice()
        );
        Ok(())
    }
}
