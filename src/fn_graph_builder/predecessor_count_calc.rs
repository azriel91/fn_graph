use std::marker::PhantomData;

use daggy::{Dag, Walker};

use crate::{Edge, FnIdInner};

/// Calculates the number of predecessors of each function.
pub(super) struct PredecessorCountCalc<F>(PhantomData<F>);

impl<F> PredecessorCountCalc<F> {
    /// Returns the number of predecessors each function has.
    pub(super) fn calc(graph: &Dag<F, Edge, FnIdInner>) -> Vec<usize> {
        graph.graph().node_indices().fold(
            vec![0usize; graph.node_count()],
            |mut predecessor_counts, fn_id| {
                graph
                    .children(fn_id)
                    .iter(graph)
                    .for_each(|(_edge_id, child_fn_id)| {
                        predecessor_counts[child_fn_id.index()] += 1;
                    });

                predecessor_counts
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use daggy::{Dag, WouldCycle};
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

        let predecessor_counts = PredecessorCountCalc::calc(&graph);

        assert_eq!([0, 0], predecessor_counts.as_slice());
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

        let predecessor_counts = PredecessorCountCalc::calc(&graph);

        assert_eq!([0, 1, 1], predecessor_counts.as_slice());
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

        let predecessor_counts = PredecessorCountCalc::calc(&graph);

        assert_eq!([1, 1, 0], predecessor_counts.as_slice());
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

        let predecessor_counts = PredecessorCountCalc::calc(&graph);

        assert_eq!([0, 1, 2, 0], predecessor_counts.as_slice());
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

        let predecessor_counts = PredecessorCountCalc::calc(&graph);

        assert_eq!([0, 1, 2, 1, 3, 0], predecessor_counts.as_slice());
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

        let predecessor_counts = PredecessorCountCalc::calc(&graph);

        assert_eq!([1, 3, 0, 0, 1, 2], predecessor_counts.as_slice());
        Ok(())
    }
}
