use daggy::{
    petgraph::{graph::NodeReferences, visit::Topo},
    Dag, Walker,
};
use serde::{Deserialize, Serialize};
use std::ops::Deref;

use crate::{Edge, FnGraph, FnIdInner};

/// Serialized representation of the function graph.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GraphInfo<NodeInfo> {
    /// The underlying directed acyclic graph.
    graph: Dag<NodeInfo, Edge, FnIdInner>,
    /// Reversed structure of the graph, with `()` as the node weights.
    graph_structure_rev: Dag<(), Edge, FnIdInner>,
}

impl<NodeInfo> GraphInfo<NodeInfo> {
    /// Returns a new `GraphInfo` by mapping each function in the given
    /// `FnGraph` into a `NodeInfo`.
    pub fn from_graph<F>(fn_graph: &FnGraph<F>, fn_info: impl Fn(&F) -> NodeInfo) -> Self {
        let mut graph = fn_graph.iter_insertion().fold(Dag::new(), |mut graph, f| {
            let node_info = fn_info(f);
            graph.add_node(node_info);
            graph
        });

        let edges = fn_graph
            .raw_edges()
            .iter()
            .map(|e| (e.source(), e.target(), e.weight));

        graph.add_edges(edges).expect(
            "Edges are all directed from the original graph, \
            so this cannot cause a cycle.",
        );
        let graph_structure_rev = fn_graph.graph_structure_rev.clone();

        Self {
            graph,
            graph_structure_rev,
        }
    }

    /// Returns an iterator of node infos in topological order.
    ///
    /// Topological order is where each node is ordered before its
    /// successors.
    ///
    /// If you want to iterate over the nodes in insertion order, see
    /// [`GraphInfo::iter_insertion`].
    pub fn iter(&self) -> impl Iterator<Item = &NodeInfo> {
        Topo::new(&self.graph)
            .iter(&self.graph)
            .map(|fn_id| &self.graph[fn_id])
    }

    /// Returns an iterator of node infos in reverse topological order.
    ///
    /// Topological order is where each node is ordered before its
    /// successors, so this returns the node infos from the leaf nodes
    /// to the root.
    ///
    /// If you want to iterate over the nodes in reverse insertion order,
    /// use [`GraphInfo::iter_insertion`], then call [`.rev()`].
    ///
    /// [`.rev()`]: std::iter::Iterator::rev
    pub fn iter_rev(&self) -> impl Iterator<Item = &NodeInfo> {
        Topo::new(&self.graph_structure_rev)
            .iter(&self.graph_structure_rev)
            .map(|fn_id| &self.graph[fn_id])
    }

    /// Returns an iterator of function references in insertion order.
    ///
    /// Each iteration returns a `(FnId, &'a F)`.
    pub fn iter_insertion_with_indices(&self) -> NodeReferences<NodeInfo, FnIdInner> {
        use daggy::petgraph::visit::IntoNodeReferences;
        self.graph.node_references()
    }
}

impl<NodeInfo> Deref for GraphInfo<NodeInfo> {
    type Target = Dag<NodeInfo, Edge, FnIdInner>;

    fn deref(&self) -> &Self::Target {
        &self.graph
    }
}

impl<NodeInfo> PartialEq for GraphInfo<NodeInfo>
where
    NodeInfo: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        // From <https://github.com/petgraph/petgraph/issues/199#issuecomment-484077775>
        let self_nodes = self.graph.raw_nodes().iter().map(|n| &n.weight);
        let other_nodes = other.graph.raw_nodes().iter().map(|n| &n.weight);
        let self_edges = self
            .graph
            .raw_edges()
            .iter()
            .map(|e| (e.source(), e.target(), &e.weight));
        let other_edges = other
            .graph
            .raw_edges()
            .iter()
            .map(|e| (e.source(), e.target(), &e.weight));
        self_nodes.eq(other_nodes) && self_edges.eq(other_edges)
    }
}

impl<NodeInfo> Eq for GraphInfo<NodeInfo> where NodeInfo: Eq {}

#[cfg(feature = "fn_meta")]
#[cfg(test)]
mod tests {
    use daggy::{petgraph::visit::IntoNodeReferences, WouldCycle};
    use resman::{FnRes, IntoFnRes, Resources};

    use super::GraphInfo;
    use crate::{Edge, FnGraph, FnGraphBuilder, FnId};

    type BoxFnRes = Box<dyn FnRes<Ret = &'static str>>;

    #[test]
    fn iter() -> Result<(), WouldCycle<Edge>> {
        let graph_info = graph_info()?;

        let node_infos = graph_info.iter().copied().collect::<Vec<&'static str>>();

        assert_eq!(["f", "a", "b", "c", "d", "e"], node_infos.as_slice());
        Ok(())
    }

    #[test]
    fn iter_rev() -> Result<(), WouldCycle<Edge>> {
        let graph_info = graph_info()?;

        let node_infos = graph_info
            .iter_rev()
            .copied()
            .collect::<Vec<&'static str>>();

        assert_eq!(["e", "d", "c", "b", "a", "f"], node_infos.as_slice());
        Ok(())
    }

    #[test]
    fn iter_insertion_with_indices() -> Result<(), WouldCycle<Edge>> {
        let graph_info = graph_info()?;

        let node_infos_with_indices = graph_info
            .iter_insertion_with_indices()
            .map(|(fn_id, node_info)| (fn_id, *node_info))
            .collect::<Vec<(FnId, &'static str)>>();

        assert_eq!(
            [
                (FnId::new(0), "a"),
                (FnId::new(1), "b"),
                (FnId::new(2), "c"),
                (FnId::new(3), "d"),
                (FnId::new(4), "e"),
                (FnId::new(5), "f"),
            ],
            node_infos_with_indices.as_slice()
        );
        Ok(())
    }

    #[test]
    fn partial_eq() -> Result<(), WouldCycle<Edge>> {
        let graph_info = graph_info()?;

        let graph_info_clone = Clone::clone(&graph_info);

        assert_eq!(graph_info, graph_info_clone);
        Ok(())
    }

    #[test]
    fn serialize() -> Result<(), Box<dyn std::error::Error>> {
        let graph_info = graph_info()?;

        assert_eq!(
            r#"graph:
  nodes:
  - a
  - b
  - c
  - d
  - e
  - f
  node_holes: []
  edge_property: directed
  edges:
  - - 0
    - 1
    - Logic
  - - 0
    - 2
    - Logic
  - - 1
    - 4
    - Logic
  - - 2
    - 3
    - Logic
  - - 3
    - 4
    - Logic
  - - 5
    - 4
    - Logic
  - - 1
    - 3
    - Data
  - - 5
    - 1
    - Data
graph_structure_rev:
  nodes:
  - null
  - null
  - null
  - null
  - null
  - null
  node_holes: []
  edge_property: directed
  edges:
  - - 1
    - 0
    - Logic
  - - 2
    - 0
    - Logic
  - - 4
    - 1
    - Logic
  - - 3
    - 2
    - Logic
  - - 4
    - 3
    - Logic
  - - 4
    - 5
    - Logic
  - - 3
    - 1
    - Data
  - - 1
    - 5
    - Data
"#,
            serde_yaml::to_string(&graph_info)?
        );
        Ok(())
    }

    #[test]
    fn deserialize() -> Result<(), Box<dyn std::error::Error>> {
        let graph_info = graph_info()?;

        assert_eq!(
            graph_info,
            serde_yaml::from_str(
                r#"graph:
  nodes: [a, b, c, d, e, f]
  node_holes: []
  edge_property: directed
  edges:
  - [0, 1, Logic]
  - [0, 2, Logic]
  - [1, 4, Logic]
  - [2, 3, Logic]
  - [3, 4, Logic]
  - [5, 4, Logic]
  - [1, 3, Data]
  - [5, 1, Data]
graph_structure_rev:
  nodes: [null, null, null, null, null, null]
  node_holes: []
  edge_property: directed
  edges:
  - [1, 0, Logic]
  - [2, 0, Logic]
  - [4, 1, Logic]
  - [3, 2, Logic]
  - [4, 3, Logic]
  - [4, 5, Logic]
  - [3, 1, Data]
  - [1, 5, Data]
"#
            )?
        );
        Ok(())
    }

    #[test]
    fn debug() -> Result<(), WouldCycle<Edge>> {
        let graph_info = graph_info()?;

        assert_eq!(
            "GraphInfo { \
                graph: Dag { graph: Graph { Ty: \"Directed\", node_count: 6, edge_count: 8, edges: (0, 1), (0, 2), (1, 4), (2, 3), (3, 4), (5, 4), (1, 3), (5, 1), node weights: {0: \"a\", 1: \"b\", 2: \"c\", 3: \"d\", 4: \"e\", 5: \"f\"}, edge weights: {0: Logic, 1: Logic, 2: Logic, 3: Logic, 4: Logic, 5: Logic, 6: Data, 7: Data} }, cycle_state: DfsSpace { dfs: Dfs { stack: [], discovered: FixedBitSet { data: [], length: 0 } } } }, \
                graph_structure_rev: Dag { graph: Graph { Ty: \"Directed\", node_count: 6, edge_count: 8, edges: (1, 0), (2, 0), (4, 1), (3, 2), (4, 3), (4, 5), (3, 1), (1, 5), edge weights: {0: Logic, 1: Logic, 2: Logic, 3: Logic, 4: Logic, 5: Logic, 6: Data, 7: Data} }, cycle_state: DfsSpace { dfs: Dfs { stack: [], discovered: FixedBitSet { data: [3], length: 6 } } } } \
            }",
            format!("{graph_info:?}")
        );
        Ok(())
    }

    #[test]
    fn deref() -> Result<(), WouldCycle<Edge>> {
        let graph_info = graph_info()?;

        let _node_references = graph_info.node_references();
        Ok(())
    }

    fn graph_info() -> Result<GraphInfo<&'static str>, WouldCycle<Edge>> {
        let mut resources = Resources::new();
        resources.insert(8u8);
        resources.insert(16u16);

        complex_graph()
            .map(|graph| GraphInfo::from_graph(&graph, |f: &BoxFnRes| f.call(&resources)))
    }

    fn complex_graph() -> Result<FnGraph<BoxFnRes>, WouldCycle<Edge>> {
        // a - b --------- e
        //   \          / /
        //    '-- c - d  /
        //              /
        //   f --------'
        //
        // `b`, `d`, and `f` all require `&mut u16`
        //
        // Data edges augmented for `b -> d`,`f -> b`
        let mut fn_graph_builder = FnGraphBuilder::new();
        let [fn_id_a, fn_id_b, fn_id_c, fn_id_d, fn_id_e, fn_id_f] = fn_graph_builder.add_fns([
            (|_: &u8| "a").into_fn_res(),
            (|_: &mut u16| "b").into_fn_res(),
            (|| "c").into_fn_res(),
            (|_: &u8, _: &mut u16| "d").into_fn_res(),
            (|| "e").into_fn_res(),
            (|_: &mut u16| "f").into_fn_res(),
        ]);
        fn_graph_builder.add_logic_edges([
            (fn_id_a, fn_id_b),
            (fn_id_a, fn_id_c),
            (fn_id_b, fn_id_e),
            (fn_id_c, fn_id_d),
            (fn_id_d, fn_id_e),
            (fn_id_f, fn_id_e),
        ])?;
        let fn_graph = fn_graph_builder.build();
        Ok(fn_graph)
    }
}
