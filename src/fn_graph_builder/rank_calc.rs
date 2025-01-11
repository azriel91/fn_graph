use std::{cmp, collections::VecDeque, marker::PhantomData};

use daggy2::{petgraph::visit::IntoNodeReferences, Dag, Walker};

use crate::{Edge, FnId, FnIdInner, Rank};

/// Calculates the rank of each function.
pub(super) struct RankCalc<F>(PhantomData<F>);

impl<F> RankCalc<F> {
    /// Returns the rank of each function.
    ///
    /// * Root nodes will have rank 0.
    /// * Nodes that are one edge away from the root will have rank 1.
    /// * Nodes that are two edges away from the root will have rank 2.
    /// * For nodes with multiple paths from the root, the maximum path length
    ///   is the rank.
    pub(super) fn calc(graph: &Dag<F, Edge, FnIdInner>) -> Vec<Rank> {
        let mut ranks = vec![Rank(0); graph.node_count()];

        // Begin with root nodes
        let mut fn_ids = graph
            .node_references()
            .filter_map(|(fn_id, _function)| {
                if Self::is_root_node(graph, fn_id) {
                    Some(fn_id)
                } else {
                    None
                }
            })
            .collect::<VecDeque<FnId>>();

        while let Some(fn_id) = fn_ids.pop_front() {
            let fn_rank = ranks[fn_id.index()];
            let child_rank_maybe = fn_rank + 1;

            graph
                .children(fn_id)
                .iter(graph)
                .for_each(|(_edge_id, child_fn_id)| {
                    let child_rank_existing = ranks[child_fn_id.index()];

                    // Update child rank to be the greater of any previously calculated rank, and
                    // the rank computed from this iteration.
                    ranks[child_fn_id.index()] = cmp::max(child_rank_existing, child_rank_maybe);

                    fn_ids.push_back(child_fn_id);
                });
        }

        ranks
    }

    fn is_root_node(graph: &Dag<F, Edge, FnIdInner>, fn_id: FnId) -> bool {
        graph.parents(fn_id).walk_next(graph).is_none()
    }
}

#[cfg(test)]
mod tests {
    use daggy2::{Dag, WouldCycle};
    use resman::IntoFnRes;

    use super::RankCalc;
    use crate::{Edge, FnIdInner, Rank};

    #[test]
    fn rank_calculation_no_edges() {
        // a
        // b
        let mut graph = Dag::<_, Edge, FnIdInner>::new();
        graph.add_node((|| {}).into_fn_res());
        graph.add_node((|| {}).into_fn_res());

        let ranks = RankCalc::calc(&graph);

        assert_eq!([Rank(0), Rank(0)], ranks.as_slice());
    }

    #[test]
    fn rank_calculation_nodes_in_order() -> Result<(), WouldCycle<Edge>> {
        // a - b - c
        let mut graph = Dag::<_, Edge, FnIdInner>::new();
        let fn_id_a = graph.add_node((|| {}).into_fn_res());
        let fn_id_b = graph.add_node((|| {}).into_fn_res());
        let fn_id_c = graph.add_node((|| {}).into_fn_res());
        graph.update_edge(fn_id_a, fn_id_b, Edge::Logic)?;
        graph.update_edge(fn_id_b, fn_id_c, Edge::Logic)?;

        let ranks = RankCalc::calc(&graph);

        assert_eq!([Rank(0), Rank(1), Rank(2)], ranks.as_slice());
        Ok(())
    }

    #[test]
    fn rank_calculation_nodes_reverse_order() -> Result<(), WouldCycle<Edge>> {
        // c - b - a
        let mut graph = Dag::<_, Edge, FnIdInner>::new();
        let fn_id_a = graph.add_node((|| {}).into_fn_res());
        let fn_id_b = graph.add_node((|| {}).into_fn_res());
        let fn_id_c = graph.add_node((|| {}).into_fn_res());
        graph.update_edge(fn_id_c, fn_id_b, Edge::Logic)?;
        graph.update_edge(fn_id_b, fn_id_a, Edge::Logic)?;

        let ranks = RankCalc::calc(&graph);

        assert_eq!([Rank(2), Rank(1), Rank(0)], ranks.as_slice());
        Ok(())
    }

    #[test]
    fn rank_calculation_multi_parent() -> Result<(), WouldCycle<Edge>> {
        // a - b - c
        //        /
        // d ----'
        let mut graph = Dag::<_, Edge, FnIdInner>::new();
        let fn_id_a = graph.add_node((|| {}).into_fn_res());
        let fn_id_b = graph.add_node((|| {}).into_fn_res());
        let fn_id_c = graph.add_node((|| {}).into_fn_res());
        let fn_id_d = graph.add_node((|| {}).into_fn_res());
        graph.update_edge(fn_id_a, fn_id_b, Edge::Logic)?;
        graph.update_edge(fn_id_b, fn_id_c, Edge::Logic)?;
        graph.update_edge(fn_id_d, fn_id_c, Edge::Logic)?;

        let ranks = RankCalc::calc(&graph);

        assert_eq!([Rank(0), Rank(1), Rank(2), Rank(0)], ranks.as_slice());
        Ok(())
    }

    #[test]
    fn rank_calculation_multi_parent_complex() -> Result<(), WouldCycle<Edge>> {
        // a - b --------- e
        //   \   \      / /
        //    '-- c - d  /
        //              /
        //   f --------'
        let mut graph = Dag::<_, Edge, FnIdInner>::new();
        let fn_id_a = graph.add_node((|| {}).into_fn_res());
        let fn_id_b = graph.add_node((|| {}).into_fn_res());
        let fn_id_c = graph.add_node((|| {}).into_fn_res());
        let fn_id_d = graph.add_node((|| {}).into_fn_res());
        let fn_id_e = graph.add_node((|| {}).into_fn_res());
        let fn_id_f = graph.add_node((|| {}).into_fn_res());
        graph.update_edge(fn_id_a, fn_id_b, Edge::Logic)?;
        graph.update_edge(fn_id_a, fn_id_c, Edge::Logic)?;
        graph.update_edge(fn_id_b, fn_id_c, Edge::Logic)?;
        graph.update_edge(fn_id_b, fn_id_e, Edge::Logic)?;
        graph.update_edge(fn_id_c, fn_id_d, Edge::Logic)?;
        graph.update_edge(fn_id_d, fn_id_e, Edge::Logic)?;
        graph.update_edge(fn_id_f, fn_id_e, Edge::Logic)?;

        let ranks = RankCalc::calc(&graph);

        assert_eq!(
            [Rank(0), Rank(1), Rank(2), Rank(3), Rank(4), Rank(0)],
            ranks.as_slice()
        );
        Ok(())
    }

    #[test]
    fn rank_calculation_multi_parent_complex_out_of_order() -> Result<(), WouldCycle<Edge>> {
        // c - a --------- b
        //   \   \      / /
        //    '-- f - e  /
        //              /
        //   d --------'
        let mut graph = Dag::<_, Edge, FnIdInner>::new();
        let fn_id_a = graph.add_node((|| {}).into_fn_res());
        let fn_id_b = graph.add_node((|| {}).into_fn_res());
        let fn_id_c = graph.add_node((|| {}).into_fn_res());
        let fn_id_d = graph.add_node((|| {}).into_fn_res());
        let fn_id_e = graph.add_node((|| {}).into_fn_res());
        let fn_id_f = graph.add_node((|| {}).into_fn_res());
        graph.update_edge(fn_id_c, fn_id_a, Edge::Logic)?;
        graph.update_edge(fn_id_c, fn_id_f, Edge::Logic)?;
        graph.update_edge(fn_id_a, fn_id_f, Edge::Logic)?;
        graph.update_edge(fn_id_a, fn_id_b, Edge::Logic)?;
        graph.update_edge(fn_id_f, fn_id_e, Edge::Logic)?;
        graph.update_edge(fn_id_e, fn_id_b, Edge::Logic)?;
        graph.update_edge(fn_id_d, fn_id_b, Edge::Logic)?;

        let ranks = RankCalc::calc(&graph);

        assert_eq!(
            [Rank(1), Rank(4), Rank(0), Rank(0), Rank(3), Rank(2)],
            ranks.as_slice()
        );
        Ok(())
    }
}
