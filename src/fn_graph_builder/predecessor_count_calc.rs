use std::marker::PhantomData;

use daggy2::{Dag, Walker};

use crate::{Edge, EdgeCounts, FnIdInner};

/// Calculates the number of predecessors of each function.
pub(super) struct PredecessorCountCalc<F>(PhantomData<F>);

impl<F> PredecessorCountCalc<F> {
    /// Returns the number of predecessors each function has.
    pub(super) fn calc(graph: &Dag<F, Edge, FnIdInner>) -> EdgeCounts {
        let (incoming, outgoing) = graph.graph().node_indices().fold(
            (
                vec![0usize; graph.node_count()],
                vec![0usize; graph.node_count()],
            ),
            |(mut incoming, mut outgoing), fn_id| {
                graph
                    .children(fn_id)
                    .iter(graph)
                    .for_each(|(_edge_id, child_fn_id)| {
                        incoming[child_fn_id.index()] += 1;
                    });
                graph
                    .parents(fn_id)
                    .iter(graph)
                    .for_each(|(_edge_id, parent_fn_id)| {
                        outgoing[parent_fn_id.index()] += 1;
                    });

                (incoming, outgoing)
            },
        );
        EdgeCounts::new(incoming, outgoing)
    }
}

#[cfg(test)]
mod tests {
    use daggy2::{Dag, WouldCycle};
    use resman::IntoFnRes;

    use super::PredecessorCountCalc;
    use crate::{Edge, FnIdInner};

    #[test]
    fn predecessor_count_calc_no_edges() {
        // a
        // b
        let mut graph = Dag::<_, Edge, FnIdInner>::new();
        graph.add_node((|| {}).into_fn_res());
        graph.add_node((|| {}).into_fn_res());

        let edge_counts = PredecessorCountCalc::calc(&graph);

        assert_eq!([0, 0], edge_counts.incoming());
    }

    #[test]
    fn predecessor_count_calc_nodes_in_order() -> Result<(), WouldCycle<Edge>> {
        // a - b - c
        let mut graph = Dag::<_, Edge, FnIdInner>::new();
        let fn_id_a = graph.add_node((|| {}).into_fn_res());
        let fn_id_b = graph.add_node((|| {}).into_fn_res());
        let fn_id_c = graph.add_node((|| {}).into_fn_res());
        graph.update_edge(fn_id_a, fn_id_b, Edge::Logic)?;
        graph.update_edge(fn_id_b, fn_id_c, Edge::Logic)?;

        let edge_counts = PredecessorCountCalc::calc(&graph);

        assert_eq!([0, 1, 1], edge_counts.incoming());
        Ok(())
    }

    #[test]
    fn predecessor_count_calc_nodes_reverse_order() -> Result<(), WouldCycle<Edge>> {
        // c - b - a
        let mut graph = Dag::<_, Edge, FnIdInner>::new();
        let fn_id_a = graph.add_node((|| {}).into_fn_res());
        let fn_id_b = graph.add_node((|| {}).into_fn_res());
        let fn_id_c = graph.add_node((|| {}).into_fn_res());
        graph.update_edge(fn_id_c, fn_id_b, Edge::Logic)?;
        graph.update_edge(fn_id_b, fn_id_a, Edge::Data)?;

        let edge_counts = PredecessorCountCalc::calc(&graph);

        assert_eq!([1, 1, 0], edge_counts.incoming());
        Ok(())
    }

    #[test]
    fn predecessor_count_calc_multi_parent() -> Result<(), WouldCycle<Edge>> {
        // a - b - c
        //        /
        // d ----'
        let mut graph = Dag::<_, Edge, FnIdInner>::new();
        let fn_id_a = graph.add_node((|| {}).into_fn_res());
        let fn_id_b = graph.add_node((|| {}).into_fn_res());
        let fn_id_c = graph.add_node((|| {}).into_fn_res());
        let fn_id_d = graph.add_node((|| {}).into_fn_res());
        graph.update_edge(fn_id_a, fn_id_b, Edge::Logic)?;
        graph.update_edge(fn_id_b, fn_id_c, Edge::Data)?;
        graph.update_edge(fn_id_d, fn_id_c, Edge::Logic)?;

        let edge_counts = PredecessorCountCalc::calc(&graph);

        assert_eq!([0, 1, 2, 0], edge_counts.incoming());
        Ok(())
    }

    #[test]
    fn predecessor_count_calc_multi_parent_complex() -> Result<(), WouldCycle<Edge>> {
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
        graph.update_edge(fn_id_b, fn_id_e, Edge::Data)?;
        graph.update_edge(fn_id_c, fn_id_d, Edge::Logic)?;
        graph.update_edge(fn_id_d, fn_id_e, Edge::Data)?;
        graph.update_edge(fn_id_f, fn_id_e, Edge::Data)?;

        let edge_counts = PredecessorCountCalc::calc(&graph);

        assert_eq!([0, 1, 2, 1, 3, 0], edge_counts.incoming());
        Ok(())
    }

    #[test]
    fn predecessor_count_calc_multi_parent_complex_out_of_order() -> Result<(), WouldCycle<Edge>> {
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
        graph.update_edge(fn_id_a, fn_id_b, Edge::Data)?;
        graph.update_edge(fn_id_f, fn_id_e, Edge::Logic)?;
        graph.update_edge(fn_id_e, fn_id_b, Edge::Data)?;
        graph.update_edge(fn_id_d, fn_id_b, Edge::Data)?;

        let edge_counts = PredecessorCountCalc::calc(&graph);

        assert_eq!([1, 3, 0, 0, 1, 2], edge_counts.incoming());
        Ok(())
    }
}
