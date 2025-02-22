use daggy::{Dag, WouldCycle};

use crate::{DataAccessDyn, Edge, EdgeId, FnGraph, FnId, FnIdInner};

#[cfg(feature = "async")]
use self::predecessor_count_calc::PredecessorCountCalc;
use self::{data_edge_augmenter::DataEdgeAugmenter, rank_calc::RankCalc};

mod data_edge_augmenter;
#[cfg(feature = "async")]
mod predecessor_count_calc;
mod rank_calc;

/// Builder for a [`FnGraph`].
#[derive(Debug)]
pub struct FnGraphBuilder<F> {
    /// Directed acyclic graph of functions.
    graph: Dag<F, Edge, FnIdInner>,
}

impl<F> FnGraphBuilder<F>
where
    F: DataAccessDyn,
{
    /// Returns a new `FnGraphBuilder`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a function to the graph.
    ///
    /// The returned function ID is used to specify dependencies between
    /// functions through the [`add_logic_edge`], [`add_contains_edge`] methods.
    ///
    /// [`add_logic_edge`]: Self::add_logic_edge
    /// [`add_contains_edge`]: Self::add_contains_edge
    pub fn add_fn(&mut self, f: F) -> FnId {
        self.graph.add_node(f)
    }

    /// Adds multiple functions to the graph.
    ///
    /// The returned function IDs are used to specify dependencies between
    /// functions through the [`add_logic_edge`] / [`add_logic_edges`] method.
    ///
    /// [`add_logic_edge`]: Self::add_logic_edge
    /// [`add_logic_edges`]: Self::add_logic_edges
    pub fn add_fns<const N: usize>(&mut self, fns: [F; N]) -> [FnId; N] {
        let mut fn_ids: [FnId; N] = [FnId::default(); N];
        IntoIterator::into_iter(fns)
            .map(|f| self.add_fn(f))
            .zip(fn_ids.iter_mut())
            .for_each(|(n, fn_id)| *fn_id = n);

        fn_ids
    }

    /// Adds a logic edge from one function to another.
    ///
    /// This differs from `petgraph`'s [`add_edge`] in that this only
    /// allows one edge between two functions. When this function is called
    /// multiple times with the same functions, only the last call's edge
    /// will be retained.
    ///
    /// [`add_edge`]: daggy::petgraph::data::Build::add_edge
    pub fn add_logic_edge(
        &mut self,
        function_from: FnId,
        function_to: FnId,
    ) -> Result<EdgeId, WouldCycle<Edge>> {
        // Use `update_edge` instead of `add_logic_edge` to avoid duplicate edges from
        // one function to the other.
        self.graph
            .update_edge(function_from, function_to, Edge::Logic)
    }

    /// Adds a contains edge from one function to another.
    ///
    /// This differs from `petgraph`'s [`add_edge`] in that this only
    /// allows one edge between two functions. When this function is called
    /// multiple times with the same functions, only the last call's edge
    /// will be retained.
    ///
    /// [`add_edge`]: daggy::petgraph::data::Build::add_edge
    pub fn add_contains_edge(
        &mut self,
        function_from: FnId,
        function_to: FnId,
    ) -> Result<EdgeId, WouldCycle<Edge>> {
        // Use `update_edge` instead of `add_logic_edge` to avoid duplicate edges from
        // one function to the other.
        self.graph
            .update_edge(function_from, function_to, Edge::Contains)
    }

    /// Adds logic edges between functions.
    pub fn add_logic_edges<const N: usize>(
        &mut self,
        edges: [(FnId, FnId); N],
    ) -> Result<[EdgeId; N], WouldCycle<Edge>> {
        let mut edge_ids: [EdgeId; N] = [EdgeId::default(); N];
        IntoIterator::into_iter(edges)
            .zip(edge_ids.iter_mut())
            .try_for_each(|((fn_from, fn_to), edge_id)| {
                self.add_logic_edge(fn_from, fn_to).map(|edge_index| {
                    *edge_id = edge_index;
                })
            })?;

        Ok(edge_ids)
    }

    /// Adds contains edges between functions.
    pub fn add_contains_edges<const N: usize>(
        &mut self,
        edges: [(FnId, FnId); N],
    ) -> Result<[EdgeId; N], WouldCycle<Edge>> {
        let mut edge_ids: [EdgeId; N] = [EdgeId::default(); N];
        IntoIterator::into_iter(edges)
            .zip(edge_ids.iter_mut())
            .try_for_each(|((fn_from, fn_to), edge_id)| {
                self.add_contains_edge(fn_from, fn_to).map(|edge_index| {
                    *edge_id = edge_index;
                })
            })?;

        Ok(edge_ids)
    }

    /// Builds and returns the [`FnGraph`].
    pub fn build(self) -> FnGraph<F> {
        let Self { mut graph } = self;
        let ranks = RankCalc::calc(&graph);
        DataEdgeAugmenter::augment(&mut graph, &ranks);
        #[cfg(feature = "async")]
        let edge_counts = PredecessorCountCalc::calc(&graph);

        let mut graph_structure = Dag::<(), Edge, FnIdInner>::new();
        let mut graph_structure_rev = Dag::<(), Edge, FnIdInner>::new();
        graph.raw_nodes().iter().for_each(|_| {
            graph_structure.add_node(());
            graph_structure_rev.add_node(());
        });
        graph
            .raw_edges()
            .iter()
            .try_for_each(|edge| {
                graph_structure
                    .add_edge(edge.source(), edge.target(), edge.weight)
                    .map(|_| ())?;
                graph_structure_rev
                    .add_edge(edge.target(), edge.source(), edge.weight)
                    .map(|_| ())
            })
            .expect("Expected no cycles to be present.");

        FnGraph {
            graph,
            graph_structure,
            graph_structure_rev,
            ranks,
            #[cfg(feature = "async")]
            edge_counts,
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

#[cfg(feature = "fn_meta")]
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
    fn add_logic_edge() -> Result<(), WouldCycle<Edge>> {
        let mut fn_graph_builder = FnGraphBuilder::new();
        let fn_a = fn_graph_builder.add_fn((|| {}).into_fn_res());
        let fn_b = fn_graph_builder.add_fn((|_: &usize| {}).into_fn_res());
        let fn_c = fn_graph_builder.add_fn((|_: &mut usize, _: &mut u32| {}).into_fn_res());
        let _fn_d = fn_graph_builder.add_fn((|_: &usize, _: &u32| {}).into_fn_res());
        fn_graph_builder.add_logic_edge(fn_a, fn_b)?;
        fn_graph_builder.add_logic_edge(fn_b, fn_c)?;

        let fn_graph = fn_graph_builder.build();

        assert_eq!(
            &[Rank(0), Rank(1), Rank(2), Rank(0)],
            fn_graph.ranks.as_slice()
        );
        Ok(())
    }

    #[test]
    fn add_logic_edges() -> Result<(), WouldCycle<Edge>> {
        let mut fn_graph_builder = FnGraphBuilder::new();
        let fn_a = fn_graph_builder.add_fn((|| {}).into_fn_res());
        let fn_b = fn_graph_builder.add_fn((|_: &usize| {}).into_fn_res());
        let fn_c = fn_graph_builder.add_fn((|_: &mut usize, _: &mut u32| {}).into_fn_res());
        let _fn_d = fn_graph_builder.add_fn((|_: &usize, _: &u32| {}).into_fn_res());
        fn_graph_builder.add_logic_edges([(fn_a, fn_b), (fn_b, fn_c)])?;

        let fn_graph = fn_graph_builder.build();

        assert_eq!(
            &[Rank(0), Rank(1), Rank(2), Rank(0)],
            fn_graph.ranks.as_slice()
        );
        Ok(())
    }
}
