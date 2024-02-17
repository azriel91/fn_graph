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
