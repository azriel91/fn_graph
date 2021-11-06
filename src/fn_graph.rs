use std::ops::{Deref, DerefMut};

use daggy::{petgraph::graph::NodeReferences, Dag, NodeWeightsMut};

use crate::{Edge, FnId, FnIdInner, Rank};

/// Directed acyclic graph of functions.
///
/// Besides the `iter_insertion*` function, all iteration functions run using
/// topological ordering -- where each function is ordered before its
/// successors.
#[derive(Clone, Debug)]
pub struct FnGraph<F> {
    /// Graph of functions.
    pub graph: Dag<F, Edge, FnIdInner>,
    /// Rank of each function.
    ///
    /// The `FnId` is guaranteed to match the index in the `Vec` as we never
    /// remove from the graph. So we don't need to use a `HashMap` here.
    pub(crate) ranks: Vec<Rank>,
    /// List of function IDs in topological order.
    ///
    /// Topological order is where each function is ordered before its
    /// successors.
    pub(crate) toposort: Vec<FnId>,
}

impl<F> FnGraph<F> {
    /// Returns an empty graph of functions.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the ranks of each function.
    pub fn ranks(&self) -> &[Rank] {
        &self.ranks
    }

    /// Returns the topological sorting of each function.
    ///
    /// Topological order is where each function is ordered before its
    /// successors.
    pub fn toposort(&self) -> &[FnId] {
        &self.toposort
    }

    /// Returns an iterator of function references in topological order.
    ///
    /// Topological order is where each function is ordered before its
    /// successors.
    ///
    /// If you want to iterate over the functions in insertion order, see
    /// [`FnGraph::iter_insertion`];
    pub fn iter(&self) -> impl Iterator<Item = &F> + ExactSizeIterator + DoubleEndedIterator {
        self.toposort
            .iter()
            .copied()
            .map(|fn_id| &self.graph[fn_id])
    }

    /// Returns an iterator that runs the provided logic over the functions.
    pub fn map<'f, FnMap, FnMapRet>(
        &'f mut self,
        mut fn_map: FnMap,
    ) -> impl Iterator<Item = FnMapRet> + ExactSizeIterator + DoubleEndedIterator + 'f
    where
        FnMap: FnMut(&mut F) -> FnMapRet + 'f,
    {
        let graph = &mut self.graph;
        self.toposort.iter().copied().map(move |fn_id| {
            let r#fn = &mut graph[fn_id];
            fn_map(r#fn)
        })
    }

    /// Runs the provided logic with every function and accumulates the result.
    pub fn fold<Seed, FnFold>(&mut self, seed: Seed, mut fn_fold: FnFold) -> Seed
    where
        FnFold: FnMut(Seed, &mut F) -> Seed,
    {
        self.toposort.iter().copied().fold(seed, |seed, fn_id| {
            let r#fn = &mut self.graph[fn_id];
            fn_fold(seed, r#fn)
        })
    }

    /// Runs the provided logic with every function and accumulates the result,
    /// stopping on the first error.
    pub fn try_fold<Seed, FnFold, E>(&mut self, seed: Seed, mut fn_fold: FnFold) -> Result<Seed, E>
    where
        FnFold: FnMut(Seed, &mut F) -> Result<Seed, E>,
    {
        self.toposort.iter().copied().try_fold(seed, |seed, fn_id| {
            let r#fn = &mut self.graph[fn_id];
            fn_fold(seed, r#fn)
        })
    }

    /// Runs the provided logic over the functions.
    pub fn for_each<FnForEach>(&mut self, mut fn_for_each: FnForEach)
    where
        FnForEach: FnMut(&mut F),
    {
        self.toposort.iter().copied().for_each(|fn_id| {
            let r#fn = &mut self.graph[fn_id];
            fn_for_each(r#fn)
        })
    }

    /// Runs the provided logic over the functions, stopping on the first error.
    pub fn try_for_each<FnForEach, E>(&mut self, mut fn_for_each: FnForEach) -> Result<(), E>
    where
        FnForEach: FnMut(&mut F) -> Result<(), E>,
    {
        self.toposort.iter().copied().try_for_each(|fn_id| {
            let r#fn = &mut self.graph[fn_id];
            fn_for_each(r#fn)
        })
    }

    /// Returns an iterator of function references in insertion order.
    ///
    /// To iterate in logical dependency order, see [`FnGraph::iter`].
    pub fn iter_insertion(
        &self,
    ) -> impl Iterator<Item = &F> + ExactSizeIterator + DoubleEndedIterator {
        use daggy::petgraph::visit::IntoNodeReferences;
        self.graph.node_references().map(|(_, function)| function)
    }

    /// Returns an iterator of mutable function references in insertion order.
    ///
    /// Return type should behave like: `impl Iterator<Item = &mut F>`.
    pub fn iter_insertion_mut(&mut self) -> NodeWeightsMut<F, FnIdInner> {
        self.graph.node_weights_mut()
    }

    /// Returns an iterator of function references in insertion order.
    ///
    /// Each iteration returns a `(FnId, &'a F)`.
    pub fn iter_insertion_with_indices(&self) -> NodeReferences<F, FnIdInner> {
        use daggy::petgraph::visit::IntoNodeReferences;
        self.graph.node_references()
    }
}

impl<F> Default for FnGraph<F> {
    fn default() -> Self {
        Self {
            graph: Dag::new(),
            ranks: Vec::new(),
            toposort: Vec::new(),
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
    use daggy::WouldCycle;
    use resman::{FnRes, IntoFnRes, Resources};

    use super::FnGraph;
    use crate::{Edge, FnGraphBuilder, FnId};

    #[test]
    fn new_returns_empty_graph() {
        let fn_graph = FnGraph::<Box<dyn FnRes<Ret = ()>>>::new();

        assert_eq!(0, fn_graph.node_count());
        assert!(fn_graph.ranks().is_empty());
        assert!(fn_graph.toposort().is_empty());
    }

    #[test]
    fn iter_returns_fns_in_dep_order() -> Result<(), WouldCycle<Edge>> {
        let fn_graph = complex_graph()?;

        let mut resources = Resources::new();
        resources.insert(0u8);
        resources.insert(0u16);
        let fn_iter_order = fn_graph
            .iter()
            .map(|f| f.call(&resources))
            .collect::<Vec<_>>();

        // Note: the `toposort` algorithm is provided by `petgraph`, and it appears to
        // inverse the order of same-ranked nodes.
        assert_eq!(["f", "a", "c", "b", "d", "e"], fn_iter_order.as_slice());
        Ok(())
    }

    #[test]
    fn map_iterates_over_fns_in_dep_order() -> Result<(), WouldCycle<Edge>> {
        let mut fn_graph = complex_graph()?;

        let mut resources = Resources::new();
        resources.insert(0u8);
        resources.insert(0u16);
        let fn_iter_order = fn_graph.map(|f| f.call(&resources)).collect::<Vec<_>>();

        assert_eq!(["f", "a", "c", "b", "d", "e"], fn_iter_order.as_slice());
        Ok(())
    }

    #[test]
    fn fold_iterates_over_fns_in_dep_order() -> Result<(), WouldCycle<Edge>> {
        let mut fn_graph = complex_graph()?;

        let mut resources = Resources::new();
        resources.insert(0u8);
        resources.insert(0u16);
        let fn_iter_order = fn_graph.fold(
            Vec::with_capacity(fn_graph.node_count()),
            |mut fn_iter_order, f| {
                fn_iter_order.push(f.call(&resources));
                fn_iter_order
            },
        );

        assert_eq!(["f", "a", "c", "b", "d", "e"], fn_iter_order.as_slice());
        Ok(())
    }

    #[test]
    fn try_fold_iterates_over_fns_in_dep_order() -> Result<(), WouldCycle<Edge>> {
        let mut fn_graph = complex_graph()?;

        let mut resources = Resources::new();
        resources.insert(0u8);
        resources.insert(0u16);
        let fn_iter_order = fn_graph.try_fold(
            Vec::with_capacity(fn_graph.node_count()),
            |mut fn_iter_order, f| {
                fn_iter_order.push(f.call(&resources));
                Ok(fn_iter_order)
            },
        )?;

        assert_eq!(["f", "a", "c", "b", "d", "e"], fn_iter_order.as_slice());
        Ok(())
    }

    #[test]
    fn for_each_iterates_over_fns_in_dep_order() -> Result<(), WouldCycle<Edge>> {
        let mut fn_graph = complex_graph()?;

        let mut resources = Resources::new();
        resources.insert(0u8);
        resources.insert(0u16);

        let mut fn_iter_order = Vec::with_capacity(fn_graph.node_count());
        fn_graph.for_each(|f| fn_iter_order.push(f.call(&resources)));

        assert_eq!(["f", "a", "c", "b", "d", "e"], fn_iter_order.as_slice());
        Ok(())
    }

    #[test]
    fn try_for_each_iterates_over_fns_in_dep_order() -> Result<(), WouldCycle<Edge>> {
        let mut fn_graph = complex_graph()?;

        let mut resources = Resources::new();
        resources.insert(0u8);
        resources.insert(0u16);

        let mut fn_iter_order = Vec::with_capacity(fn_graph.node_count());
        fn_graph.try_for_each(|f| Ok(fn_iter_order.push(f.call(&resources))))?;

        assert_eq!(["f", "a", "c", "b", "d", "e"], fn_iter_order.as_slice());
        Ok(())
    }

    #[test]
    fn iter_insertion_returns_fns() {
        let resources = Resources::new();
        let fn_graph = {
            let mut fn_graph_builder = FnGraphBuilder::new();
            fn_graph_builder.add_fn((|| 1).into_fn_res());
            fn_graph_builder.add_fn((|| 2).into_fn_res());
            fn_graph_builder.build()
        };

        let call_fn = |f: &Box<dyn FnRes<Ret = u32>>| f.call(&resources);
        let mut fn_iter = fn_graph.iter_insertion();
        assert_eq!(Some(1), fn_iter.next().map(call_fn));
        assert_eq!(Some(2), fn_iter.next().map(call_fn));
        assert_eq!(None, fn_iter.next().map(call_fn));
    }

    #[test]
    fn iter_insertion_mut_returns_fns() {
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
        let mut fn_iter = fn_graph.iter_insertion_mut();
        assert_eq!(Some(1), fn_iter.next().map(call_fn));
        assert_eq!(Some(2), fn_iter.next().map(call_fn));
        assert_eq!(None, fn_iter.next().map(call_fn));

        let mut fn_iter = fn_graph.iter_insertion_mut();
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
        let mut fn_iter = fn_graph.iter_insertion_with_indices();

        assert_eq!(Some((FnId::new(0), 1)), fn_iter.next().map(call_fn));
        assert_eq!(Some((FnId::new(1), 2)), fn_iter.next().map(call_fn));
        assert_eq!(None, fn_iter.next().map(call_fn));
    }

    fn complex_graph() -> Result<FnGraph<Box<dyn FnRes<Ret = &'static str>>>, WouldCycle<Edge>> {
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
        fn_graph_builder.add_edges([
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
