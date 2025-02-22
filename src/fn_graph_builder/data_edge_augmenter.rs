use std::marker::PhantomData;

use daggy::{petgraph::algo::has_path_connecting, Dag};

use crate::{DataAccessDyn, Edge, FnId, FnIdInner, Rank};

pub(super) struct DataEdgeAugmenter<F>(PhantomData<F>);

impl<F> DataEdgeAugmenter<F>
where
    F: DataAccessDyn,
{
    /// Adds forward edges between functions with data access dependencies.
    ///
    /// Edges are only added if there is no existing path (set of edges) between
    /// those functions.
    ///
    /// # Algorithm
    ///
    /// 1. Beginning with nodes of the highest [`Rank`], find unreachable nodes
    ///    whose rank is higher than the current node.
    /// 2. If there is a data access conflict -- both nodes require access to
    ///    some data, and at least one of the nodes requires mutable access --
    ///    then we add an edge.
    pub(super) fn augment(graph: &mut Dag<F, Edge, FnIdInner>, ranks: &[Rank]) {
        // FnIds that have already been seen, and so we don't have to traverse further.
        let mut fn_ids_seen = vec![false; graph.node_count()];

        let mut fn_ids = (0..graph.node_count())
            .map(FnId::new)
            .collect::<Vec<FnId>>();
        fn_ids.sort_by(|fn_id_a, fn_id_b| ranks[fn_id_a.index()].cmp(&ranks[fn_id_b.index()]));
        // Iterate in reverse order beginning from highest rank, to reduce
        // number of comparisons.
        let fn_rev_rank_iter = fn_ids.iter().copied().enumerate().rev();

        fn_rev_rank_iter.for_each(|(index, fn_id)| {
            // Need to reset `seen` values for this iteration.
            fn_ids_seen.fill(false);

            // Find subsequent nodes from the current node where there is no path.
            let fn_rank_to_end_iter = fn_ids[index..]
                .iter()
                .copied()
                .filter(|fn_id_next| fn_id != *fn_id_next);

            fn_rank_to_end_iter.for_each(|fn_id_next| {
                let fn_id_seen = fn_ids_seen[fn_id_next.index()];
                if !fn_id_seen {
                    fn_ids_seen[fn_id_next.index()] = true;

                    let are_connected = has_path_connecting(&*graph, fn_id, fn_id_next, None);
                    if !are_connected {
                        // Check if the parameters required by the functions conflict.
                        // If they do, we add an edge.

                        let r#fn = &graph[fn_id];
                        let fn_borrows = r#fn.borrows();
                        let fn_borrow_muts = r#fn.borrow_muts();

                        let fn_next = &graph[fn_id_next];
                        let fn_next_borrows = fn_next.borrows();
                        let fn_next_borrow_muts = fn_next.borrow_muts();

                        // There is a conflict when any of:
                        //
                        // * the current function is reading what the next function needs to write.
                        // * the current function is writing what the next function needs to read.
                        // * the current function is writing what the next function needs to write.
                        let conflict = fn_borrows
                            .iter()
                            .any(|left| fn_next_borrow_muts.iter().any(|right| left == right))
                            || fn_borrow_muts
                                .iter()
                                .any(|left| fn_next_borrows.iter().any(|right| left == right))
                            || fn_borrow_muts
                                .iter()
                                .any(|left| fn_next_borrow_muts.iter().any(|right| left == right));

                        if conflict {
                            graph
                                .update_edge(fn_id, fn_id_next, Edge::Data)
                                .expect("Failed to add data edge between functions.");
                        }
                    }
                }
            });
        });
    }
}

#[cfg(feature = "fn_meta")]
#[cfg(test)]
mod tests {
    use daggy::{petgraph::algo::all_simple_paths, Dag, WouldCycle};
    use resman::{FnRes, IntoFnRes};

    use super::{super::RankCalc, DataEdgeAugmenter};
    use crate::{Edge, FnId, FnIdInner};

    #[test]
    fn does_not_add_data_edge_when_no_data_deps() {
        // a
        // b
        let mut graph = Dag::<_, Edge, FnIdInner>::new();
        graph.add_node((|| {}).into_fn_res());
        graph.add_node((|| {}).into_fn_res());

        let ranks = RankCalc::calc(&graph);
        DataEdgeAugmenter::augment(&mut graph, &ranks);

        assert_eq!(0, graph.edge_count());
    }

    #[test]
    fn does_not_add_data_edge_when_ww_conflict_but_logic_edge_exists(
    ) -> Result<(), WouldCycle<Edge>> {
        // a - b
        let mut graph = Dag::<_, Edge, FnIdInner>::new();
        let fn_id_a = graph.add_node((|_: &mut u8| {}).into_fn_res());
        let fn_id_b = graph.add_node((|_: &mut u8| {}).into_fn_res());
        graph.update_edge(fn_id_a, fn_id_b, Edge::Logic)?;

        let ranks = RankCalc::calc(&graph);
        DataEdgeAugmenter::augment(&mut graph, &ranks);

        assert_edge_exists(&graph, fn_id_a, fn_id_b, Edge::Logic);
        Ok(())
    }

    #[test]
    fn adds_data_edge_when_ww_conflict_and_no_edge_exists() -> Result<(), WouldCycle<Edge>> {
        // a
        // b
        let mut graph = Dag::<_, Edge, FnIdInner>::new();
        let fn_id_a = graph.add_node((|_: &mut u8| {}).into_fn_res());
        let fn_id_b = graph.add_node((|_: &mut u8| {}).into_fn_res());

        let ranks = RankCalc::calc(&graph);
        DataEdgeAugmenter::augment(&mut graph, &ranks);

        assert_edge_exists(&graph, fn_id_a, fn_id_b, Edge::Data);
        Ok(())
    }

    #[test]
    fn adds_data_edge_when_rw_conflict_and_no_edge_exists() -> Result<(), WouldCycle<Edge>> {
        // a
        // b
        let mut graph = Dag::<_, Edge, FnIdInner>::new();
        let fn_id_a = graph.add_node((|_: &u8| {}).into_fn_res());
        let fn_id_b = graph.add_node((|_: &mut u8| {}).into_fn_res());

        let ranks = RankCalc::calc(&graph);
        DataEdgeAugmenter::augment(&mut graph, &ranks);

        assert_edge_exists(&graph, fn_id_a, fn_id_b, Edge::Data);
        Ok(())
    }

    #[test]
    fn adds_data_edge_when_wr_conflict_and_no_edge_exists() -> Result<(), WouldCycle<Edge>> {
        // a
        // b
        let mut graph = Dag::<_, Edge, FnIdInner>::new();
        let fn_id_a = graph.add_node((|_: &mut u8| {}).into_fn_res());
        let fn_id_b = graph.add_node((|_: &u8| {}).into_fn_res());

        let ranks = RankCalc::calc(&graph);
        DataEdgeAugmenter::augment(&mut graph, &ranks);

        assert_edge_exists(&graph, fn_id_a, fn_id_b, Edge::Data);
        Ok(())
    }

    #[test]
    fn adds_data_edge_when_wr_conflict_and_no_edge_exists_complex() -> Result<(), WouldCycle<Edge>>
    {
        // a - b -------- e
        //   \          /
        //    '-- c - d
        //
        // There should be a data edge added between b and d.
        let mut graph = Dag::<_, Edge, FnIdInner>::new();
        let fn_id_a = graph.add_node((|_: &u8| {}).into_fn_res());
        let fn_id_b = graph.add_node((|_: &mut u16| {}).into_fn_res());
        let fn_id_c = graph.add_node((|_: &u8| {}).into_fn_res());
        let fn_id_d = graph.add_node((|_: &u8, _: &u16| {}).into_fn_res());
        let fn_id_e = graph.add_node((|| {}).into_fn_res());
        graph.update_edge(fn_id_a, fn_id_b, Edge::Logic)?;
        graph.update_edge(fn_id_a, fn_id_c, Edge::Logic)?;
        graph.update_edge(fn_id_b, fn_id_e, Edge::Logic)?;
        graph.update_edge(fn_id_c, fn_id_d, Edge::Logic)?;
        graph.update_edge(fn_id_d, fn_id_e, Edge::Logic)?;
        assert_eq!(5, graph.edge_count());

        let ranks = RankCalc::calc(&graph);
        DataEdgeAugmenter::augment(&mut graph, &ranks);

        assert_edge_exists(&graph, fn_id_b, fn_id_d, Edge::Data);
        assert_eq!(6, graph.edge_count());
        Ok(())
    }

    #[test]
    fn adds_data_edge_when_ww_conflict_and_no_edge_exists_complex() -> Result<(), WouldCycle<Edge>>
    {
        // a - b --------- e
        //   \          / /
        //    '-- c - d  /
        //              /
        //   f --------'
        //
        // `b`, `d`, and `f` all require `&mut u16`
        //
        // Expect to add edges for `b -> d`,`f -> b`
        let mut graph = Dag::<_, Edge, FnIdInner>::new();
        let fn_id_a = graph.add_node((|_: &u8| {}).into_fn_res());
        let fn_id_b = graph.add_node((|_: &mut u16| {}).into_fn_res());
        let fn_id_c = graph.add_node((|| {}).into_fn_res());
        let fn_id_d = graph.add_node((|_: &u8, _: &mut u16| {}).into_fn_res());
        let fn_id_e = graph.add_node((|| {}).into_fn_res());
        let fn_id_f = graph.add_node((|_: &mut u16| {}).into_fn_res());
        graph.update_edge(fn_id_a, fn_id_b, Edge::Logic)?;
        graph.update_edge(fn_id_a, fn_id_c, Edge::Logic)?;
        graph.update_edge(fn_id_b, fn_id_e, Edge::Logic)?;
        graph.update_edge(fn_id_c, fn_id_d, Edge::Logic)?;
        graph.update_edge(fn_id_d, fn_id_e, Edge::Logic)?;
        graph.update_edge(fn_id_f, fn_id_e, Edge::Logic)?;
        assert_eq!(6, graph.edge_count());

        let ranks = RankCalc::calc(&graph);
        DataEdgeAugmenter::augment(&mut graph, &ranks);

        assert_edge_exists(&graph, fn_id_b, fn_id_d, Edge::Data);
        assert_edge_exists(&graph, fn_id_f, fn_id_b, Edge::Data);
        assert_eq!(8, graph.edge_count());
        Ok(())
    }

    fn assert_edge_exists(
        graph: &Dag<Box<dyn FnRes<Ret = ()>>, Edge, FnIdInner>,
        fn_id_from: FnId,
        fn_id_to: FnId,
        edge: Edge,
    ) {
        let mut paths_iter = all_simple_paths::<Vec<_>, _>(&graph, fn_id_from, fn_id_to, 0, None);
        assert_eq!(Some(vec![fn_id_from, fn_id_to]), paths_iter.next());
        assert_eq!(None, paths_iter.next());
        assert_eq!(
            Some(edge),
            graph
                .find_edge(fn_id_from, fn_id_to)
                .and_then(|edge_id| graph.edge_weight(edge_id).copied())
        );
    }
}
