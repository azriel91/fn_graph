use std::ops::{Deref, DerefMut};

use daggy::{petgraph::graph::NodeReferences, Dag, NodeWeightsMut};

use crate::{Edge, FnIdInner, Rank};

/// Directed acyclic graph of functions.
#[derive(Clone, Debug)]
pub struct FnGraph<F> {
    /// Graph of functions.
    pub graph: Dag<F, Edge, FnIdInner>,
    /// Rank of each function.
    ///
    /// The `FnId` is guaranteed to match the index in the `Vec` as we never
    /// remove from the graph. So we don't need to use a `HashMap` here.
    pub ranks: Vec<Rank>,
}

impl<F> FnGraph<F> {
    /// Returns an empty graph of functions.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns an iterator over references of all functions.
    pub fn iter(&self) -> impl Iterator<Item = &F> + ExactSizeIterator + DoubleEndedIterator {
        use daggy::petgraph::visit::IntoNodeReferences;
        self.graph.node_references().map(|(_, function)| function)
    }

    /// Returns an iterator over mutable references of all functions.
    ///
    /// Return type should behave like: `impl Iterator<Item = &mut F>`.
    pub fn iter_mut(&mut self) -> NodeWeightsMut<F, FnIdInner> {
        self.graph.node_weights_mut()
    }

    /// Returns an iterator over references of all functions.
    ///
    /// Each iteration returns a `(FnId, &'a F)`.
    pub fn iter_with_indices(&self) -> NodeReferences<F, FnIdInner> {
        use daggy::petgraph::visit::IntoNodeReferences;
        self.graph.node_references()
    }
}

impl<F> Default for FnGraph<F> {
    fn default() -> Self {
        Self {
            graph: Dag::new(),
            ranks: Vec::new(),
        }
    }
}

impl<F> Deref for FnGraph<F> {
    type Target = Dag<F, Edge, FnIdInner>;

    #[cfg(not(tarpaulin_include))]
    fn deref(&self) -> &Self::Target {
        &self.graph
    }
}

impl<F> DerefMut for FnGraph<F> {
    #[cfg(not(tarpaulin_include))]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.graph
    }
}

#[cfg(test)]
mod tests {
    use resman::{FnRes, IntoFnRes, Resources};

    use super::FnGraph;
    use crate::{FnGraphBuilder, FnId};

    #[test]
    fn new_returns_empty_graph() {
        let fn_graph = FnGraph::<Box<dyn FnRes<Ret = ()>>>::new();

        assert_eq!(0, fn_graph.node_count());
        assert!(fn_graph.ranks.is_empty());
    }

    #[test]
    fn iter_returns_fns() {
        let resources = Resources::new();
        let fn_graph = {
            let mut fn_graph_builder = FnGraphBuilder::new();
            fn_graph_builder.add_fn((|| 1).into_fn_res());
            fn_graph_builder.add_fn((|| 2).into_fn_res());
            fn_graph_builder.build()
        };

        let call_fn = |f: &Box<dyn FnRes<Ret = u32>>| f.call(&resources);
        let mut fn_iter = fn_graph.iter();
        assert_eq!(Some(1), fn_iter.next().map(call_fn));
        assert_eq!(Some(2), fn_iter.next().map(call_fn));
        assert_eq!(None, fn_iter.next().map(call_fn));
    }

    #[test]
    fn iter_mut_returns_fns() {
        let mut resources = Resources::new();
        resources.insert(0usize);
        resources.insert(0u32);
        let mut fn_graph = {
            let mut fn_graph_builder = FnGraphBuilder::new();
            fn_graph_builder.add_fn(
                (|a: &mut usize| {
                    *a += 1;
                    *a as u32
                })
                .into_fn_res(),
            );
            fn_graph_builder.add_fn(
                (|b: &mut u32| {
                    *b += 2;
                    *b
                })
                .into_fn_res(),
            );
            fn_graph_builder.build()
        };

        let call_fn = |f: &mut Box<dyn FnRes<Ret = u32>>| f.call(&resources);
        let mut fn_iter = fn_graph.iter_mut();
        assert_eq!(Some(1), fn_iter.next().map(call_fn));
        assert_eq!(Some(2), fn_iter.next().map(call_fn));
        assert_eq!(None, fn_iter.next().map(call_fn));

        let mut fn_iter = fn_graph.iter_mut();
        assert_eq!(Some(2), fn_iter.next().map(call_fn));
        assert_eq!(Some(4), fn_iter.next().map(call_fn));
        assert_eq!(None, fn_iter.next().map(call_fn));
    }

    #[test]
    fn iter_with_indicies_returns_fns_with_indicies() {
        let resources = Resources::new();
        let fn_graph = {
            let mut fn_graph_builder = FnGraphBuilder::new();
            fn_graph_builder.add_fn((|| 1u32).into_fn_res());
            fn_graph_builder.add_fn((|| 2u32).into_fn_res());
            fn_graph_builder.build()
        };

        let call_fn = |(fn_id, f): (FnId, &Box<dyn FnRes<Ret = u32>>)| (fn_id, f.call(&resources));
        let mut fn_iter = fn_graph.iter_with_indices();

        assert_eq!(Some((FnId::new(0), 1)), fn_iter.next().map(call_fn));
        assert_eq!(Some((FnId::new(1), 2)), fn_iter.next().map(call_fn));
        assert_eq!(None, fn_iter.next().map(call_fn));
    }
}
