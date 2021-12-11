use std::ops::{Deref, DerefMut};

use daggy::{
    petgraph::{graph::NodeReferences, visit::Topo},
    Dag, NodeWeightsMut,
};
use fixedbitset::FixedBitSet;

use crate::{Edge, FnId, FnIdInner, Rank};

#[cfg(feature = "async")]
use daggy::Walker;
#[cfg(feature = "async")]
use futures::task::Poll;
#[cfg(feature = "async")]
use futures::{
    future::{BoxFuture, LocalBoxFuture},
    stream::{self, Stream, StreamExt},
};
#[cfg(feature = "async")]
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    RwLock,
};

#[cfg(feature = "async")]
use crate::FnRef;

/// Directed acyclic graph of functions.
///
/// Besides the `iter_insertion*` function, all iteration functions run using
/// topological ordering -- where each function is ordered before its
/// successors.
#[derive(Clone, Debug)]
pub struct FnGraph<F> {
    /// Graph of functions.
    pub graph: Dag<F, Edge, FnIdInner>,
    /// Graph of functions.
    pub(crate) graph_structure: Dag<(), Edge, FnIdInner>,
    /// Rank of each function.
    ///
    /// The `FnId` is guaranteed to match the index in the `Vec` as we never
    /// remove from the graph. So we don't need to use a `HashMap` here.
    pub(crate) ranks: Vec<Rank>,
    /// Number of predecessors of each function.
    pub(crate) predecessor_counts: Vec<usize>,
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
    pub fn toposort(&self) -> Topo<FnId, FixedBitSet> {
        Topo::new(&self.graph)
    }

    /// Returns an iterator of function references in topological order.
    ///
    /// Topological order is where each function is ordered before its
    /// successors.
    ///
    /// If you want to iterate over the functions in insertion order, see
    /// [`FnGraph::iter_insertion`];
    pub fn iter(&self) -> impl Iterator<Item = &F> {
        Topo::new(&self.graph)
            .iter(&self.graph)
            .map(|fn_id| &self.graph[fn_id])
    }

    /// Returns a stream of function references in topological order.
    ///
    /// Functions are produced by the stream only when all of their predecessors
    /// have returned.
    #[cfg(feature = "async")]
    pub fn stream(&self) -> impl Stream<Item = FnRef<'_, F>> + '_ {
        // Decrement `predecessor_counts[child_fn_id]` every time a function ref
        // is dropped, and if `predecessor_counts[child_fn_id]` is 0, the stream
        // can produce `child_fn`s.
        let mut predecessor_counts = self.predecessor_counts.clone();
        let channel_capacity = std::cmp::max(1, self.graph.node_count());
        let (fn_ready_tx, mut fn_ready_rx) = mpsc::channel(channel_capacity);
        let (fn_done_tx, mut fn_done_rx) = mpsc::channel::<FnId>(channel_capacity);

        // Preload the channel with all of the functions that have no predecessors
        Topo::new(&self.graph)
            .iter(&self.graph)
            .filter(|fn_id| predecessor_counts[fn_id.index()] == 0)
            .try_for_each(|fn_id| fn_ready_tx.try_send(fn_id))
            .expect("Failed to preload function with no predecessors.");

        let mut fns_remaining = self.graph.node_count();
        let mut fn_ready_tx = Some(fn_ready_tx);
        let mut fn_done_tx = Some(fn_done_tx);
        stream::poll_fn(move |context| {
            match fn_done_rx.poll_recv(context) {
                Poll::Pending => {}
                Poll::Ready(None) => {}
                Poll::Ready(Some(fn_id)) => self.graph.children(fn_id).iter(&self.graph).for_each(
                    |(_edge_id, child_fn_id)| {
                        predecessor_counts[child_fn_id.index()] -= 1;
                        if predecessor_counts[child_fn_id.index()] == 0 {
                            if let Some(fn_ready_tx) = fn_ready_tx.as_ref() {
                                fn_ready_tx.try_send(child_fn_id).unwrap_or_else(|e| {
                                    panic!(
                                        "Failed to queue function `{}`. Cause: {}",
                                        fn_id.index(),
                                        e
                                    )
                                });
                            }
                        }
                    },
                ),
            }

            let poll = if let Some(fn_done_tx) = fn_done_tx.as_ref() {
                fn_ready_rx.poll_recv(context).map(|fn_id| {
                    fn_id.map(|fn_id| {
                        let r#fn = &self.graph[fn_id];
                        FnRef {
                            fn_id,
                            r#fn,
                            fn_done_tx: fn_done_tx.clone(),
                        }
                    })
                })
            } else {
                Poll::Ready(None)
            };

            // We have to track how many functions have been processed and end the stream.
            if let Poll::Ready(Some(..)) = &poll {
                fns_remaining -= 1;

                if fns_remaining == 0 {
                    fn_done_tx.take();
                    fn_ready_tx.take();
                }
            }

            poll
        })
    }

    /// Returns a stream of function references in topological order.
    ///
    /// Functions are produced by the stream only when all of their predecessors
    /// have returned.
    #[cfg(feature = "async")]
    pub async fn fold_async<'f, Seed, FnFold>(&mut self, seed: Seed, fn_fold: FnFold) -> Seed
    where
        FnFold: FnMut(Seed, &mut F) -> LocalBoxFuture<'_, Seed>,
    {
        let predecessor_counts = self.predecessor_counts.clone();
        let channel_capacity = std::cmp::max(1, self.graph.node_count());
        let (fn_ready_tx, mut fn_ready_rx) = mpsc::channel(channel_capacity);
        let (fn_done_tx, fn_done_rx) = mpsc::channel::<FnId>(channel_capacity);

        // Preload the channel with all of the functions that have no predecessors
        Topo::new(&self.graph)
            .iter(&self.graph)
            .filter(|fn_id| predecessor_counts[fn_id.index()] == 0)
            .try_for_each(|fn_id| fn_ready_tx.try_send(fn_id))
            .expect("Failed to preload function with no predecessors.");

        let queuer = Self::fn_ready_queuer(
            &self.graph_structure,
            predecessor_counts,
            fn_done_rx,
            fn_ready_tx,
        );

        let fns_remaining = self.graph.node_count();
        let graph = &mut self.graph;
        let fn_done_tx = Some(fn_done_tx);
        let scheduler = async move {
            let (_graph, _fns_remaining, _fn_done_tx, seed, _fn_fold) = stream::poll_fn(
                move |context| fn_ready_rx.poll_recv(context),
            )
            .fold(
                (graph, fns_remaining, fn_done_tx, seed, fn_fold),
                |(graph, mut fns_remaining, mut fn_done_tx, seed, mut fn_fold), fn_id| async move {
                    let r#fn = &mut graph[fn_id];
                    let seed = fn_fold(seed, r#fn).await;
                    if let Some(fn_done_tx) = fn_done_tx.as_ref() {
                        fn_done_tx
                            .send(fn_id)
                            .await
                            .expect("Scheduler failed to send fn_id in `fn_done_tx`.");
                    }

                    // Close `fn_done_rx` when all functions have been executed,
                    fns_remaining -= 1;
                    if fns_remaining == 0 {
                        fn_done_tx.take();
                    }

                    (graph, fns_remaining, fn_done_tx, seed, fn_fold)
                },
            )
            .await;

            seed
        };

        let (_, seed) = futures::join!(queuer, scheduler);

        seed
    }

    // https://users.rust-lang.org/t/lifetime-may-not-live-long-enough-for-an-async-closure/62489
    #[cfg(feature = "async")]
    pub async fn for_each_concurrent<FnForEach>(
        &mut self,
        limit: impl Into<Option<usize>>,
        fn_for_each: FnForEach,
    ) where
        FnForEach: Fn(&mut F) -> BoxFuture<'_, ()>,
    {
        let predecessor_counts = self.predecessor_counts.clone();
        let channel_capacity = std::cmp::max(1, self.graph.node_count());
        let (fn_ready_tx, mut fn_ready_rx) = mpsc::channel(channel_capacity);
        let (fn_done_tx, fn_done_rx) = mpsc::channel::<FnId>(channel_capacity);

        // Preload the channel with all of the functions that have no predecessors
        Topo::new(&self.graph)
            .iter(&self.graph)
            .filter(|fn_id| predecessor_counts[fn_id.index()] == 0)
            .try_for_each(|fn_id| fn_ready_tx.try_send(fn_id))
            .expect("Failed to preload function with no predecessors.");

        let queuer = Self::fn_ready_queuer(
            &self.graph_structure,
            predecessor_counts,
            fn_done_rx,
            fn_ready_tx,
        );

        let fn_done_tx = RwLock::new(Some(fn_done_tx));
        let fn_done_tx = &fn_done_tx;
        let fn_for_each = &fn_for_each;
        let fns_remaining = self.graph.node_count();
        let fns_remaining = RwLock::new(fns_remaining);
        let fns_remaining = &fns_remaining;
        let fn_mut_refs = self
            .graph
            .node_weights_mut()
            .map(RwLock::new)
            .collect::<Vec<_>>();
        let fn_mut_refs = &fn_mut_refs;
        let scheduler = async move {
            stream::poll_fn(move |context| fn_ready_rx.poll_recv(context))
                .for_each_concurrent(limit, |fn_id| async move {
                    let mut r#fn = fn_mut_refs[fn_id.index()]
                        .try_write()
                        .expect("Expected to borrow fn mutably.");
                    fn_for_each(&mut r#fn).await;
                    if let Some(fn_done_tx) = fn_done_tx.read().await.as_ref() {
                        fn_done_tx
                            .send(fn_id)
                            .await
                            .expect("Scheduler failed to send fn_id in `fn_done_tx`.");
                    }

                    // Close `fn_done_rx` when all functions have been executed,
                    let fns_remaining_val = {
                        let mut fns_remaining_ref = fns_remaining.write().await;
                        *fns_remaining_ref -= 1;
                        *fns_remaining_ref
                    };
                    if fns_remaining_val == 0 {
                        fn_done_tx.write().await.take();
                    }
                })
                .await;
        };

        futures::join!(queuer, scheduler);
    }

    /// Sends IDs of function whose predecessors have been executed to
    /// `fn_ready_tx`.
    async fn fn_ready_queuer(
        graph_structure: &Dag<(), Edge, FnIdInner>,
        predecessor_counts: Vec<usize>,
        mut fn_done_rx: Receiver<FnId>,
        fn_ready_tx: Sender<FnId>,
    ) {
        let fns_remaining = graph_structure.node_count();
        let fn_ready_tx = Some(fn_ready_tx);
        stream::poll_fn(move |context| fn_done_rx.poll_recv(context))
            .fold(
                (fns_remaining, predecessor_counts, fn_ready_tx),
                move |(mut fns_remaining, mut predecessor_counts, mut fn_ready_tx), fn_id| async move {
                    // Close `fn_ready_rx` when all functions have been executed,
                    fns_remaining -= 1;
                    if fns_remaining == 0 {
                        fn_ready_tx.take();
                    }

                    graph_structure
                        .children(fn_id)
                        .iter(graph_structure)
                        .for_each(|(_edge_id, child_fn_id)| {
                            predecessor_counts[child_fn_id.index()] -= 1;
                            if predecessor_counts[child_fn_id.index()] == 0 {
                                if let Some(fn_ready_tx) = fn_ready_tx.as_ref() {
                                    fn_ready_tx.try_send(child_fn_id).unwrap_or_else(|e| {
                                        panic!(
                                            "Failed to queue function `{}`. Cause: {}",
                                            fn_id.index(),
                                            e
                                        )
                                    });
                                }
                            }
                        });

                    (fns_remaining, predecessor_counts, fn_ready_tx)
                },
            )
            .await;
    }

    /// Returns an iterator that runs the provided logic over the functions.
    pub fn map<'f, FnMap, FnMapRet>(
        &'f mut self,
        mut fn_map: FnMap,
    ) -> impl Iterator<Item = FnMapRet> + 'f
    where
        FnMap: FnMut(&mut F) -> FnMapRet + 'f,
    {
        let mut topo = Topo::new(&self.graph);
        let graph = &mut self.graph;
        std::iter::from_fn(move || {
            topo.next(&*graph).map(|fn_id| {
                let r#fn = &mut graph[fn_id];
                fn_map(r#fn)
            })
        })
    }

    /// Runs the provided logic with every function and accumulates the result.
    pub fn fold<Seed, FnFold>(&mut self, mut seed: Seed, mut fn_fold: FnFold) -> Seed
    where
        FnFold: FnMut(Seed, &mut F) -> Seed,
    {
        let mut topo = Topo::new(&self.graph);
        let graph = &mut self.graph;
        while let Some(r#fn) = topo.next(&*graph).map(|fn_id| &mut graph[fn_id]) {
            seed = fn_fold(seed, r#fn);
        }
        seed
    }

    /// Runs the provided logic with every function and accumulates the result,
    /// stopping on the first error.
    pub fn try_fold<Seed, FnFold, E>(
        &mut self,
        mut seed: Seed,
        mut fn_fold: FnFold,
    ) -> Result<Seed, E>
    where
        FnFold: FnMut(Seed, &mut F) -> Result<Seed, E>,
    {
        let mut topo = Topo::new(&self.graph);
        let graph = &mut self.graph;
        while let Some(r#fn) = topo.next(&*graph).map(|fn_id| &mut graph[fn_id]) {
            seed = fn_fold(seed, r#fn)?;
        }
        Ok(seed)
    }

    /// Runs the provided logic over the functions.
    pub fn for_each<FnForEach>(&mut self, mut fn_for_each: FnForEach)
    where
        FnForEach: FnMut(&mut F),
    {
        let mut topo = Topo::new(&self.graph);
        let graph = &mut self.graph;
        while let Some(r#fn) = topo.next(&*graph).map(|fn_id| &mut graph[fn_id]) {
            fn_for_each(r#fn);
        }
    }

    /// Runs the provided logic over the functions, stopping on the first error.
    pub fn try_for_each<FnForEach, E>(&mut self, mut fn_for_each: FnForEach) -> Result<(), E>
    where
        FnForEach: FnMut(&mut F) -> Result<(), E>,
    {
        let mut topo = Topo::new(&self.graph);
        let graph = &mut self.graph;
        while let Some(r#fn) = topo.next(&*graph).map(|fn_id| &mut graph[fn_id]) {
            fn_for_each(r#fn)?;
        }
        Ok(())
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
            graph_structure: Dag::new(),
            ranks: Vec::new(),
            predecessor_counts: Vec::new(),
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

        assert_eq!(["f", "a", "b", "c", "d", "e"], fn_iter_order.as_slice());
        Ok(())
    }

    #[test]
    fn map_iterates_over_fns_in_dep_order() -> Result<(), WouldCycle<Edge>> {
        let mut fn_graph = complex_graph()?;

        let mut resources = Resources::new();
        resources.insert(0u8);
        resources.insert(0u16);
        let fn_iter_order = fn_graph.map(|f| f.call(&resources)).collect::<Vec<_>>();

        assert_eq!(["f", "a", "b", "c", "d", "e"], fn_iter_order.as_slice());
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

        assert_eq!(["f", "a", "b", "c", "d", "e"], fn_iter_order.as_slice());
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

        assert_eq!(["f", "a", "b", "c", "d", "e"], fn_iter_order.as_slice());
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

        assert_eq!(["f", "a", "b", "c", "d", "e"], fn_iter_order.as_slice());
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

        assert_eq!(["f", "a", "b", "c", "d", "e"], fn_iter_order.as_slice());
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

    #[cfg(feature = "async")]
    mod async_tests {
        use daggy::WouldCycle;
        use futures::{future::BoxFuture, stream, FutureExt, StreamExt};
        use resman::{FnRes, FnResMut, IntoFnRes, IntoFnResMut, Resources};
        use tokio::{
            runtime,
            sync::mpsc::{self, Receiver},
            time::{self, Duration},
        };

        use super::super::FnGraph;
        use crate::{Edge, FnGraphBuilder};

        #[test]
        fn stream_returns_fns_in_dep_order_concurrently() -> Result<(), Box<dyn std::error::Error>>
        {
            let rt = runtime::Builder::new_current_thread()
                .enable_time()
                .build()?;
            rt.block_on(async {
                let (fn_graph, mut seq_rx) = complex_graph_unit()?;

                let mut resources = Resources::new();
                resources.insert(0u8);
                resources.insert(0u16);
                let resources = &resources;
                fn_graph
                    .stream()
                    .for_each_concurrent(4, |f| async move { f.call(resources).await })
                    .await;

                let fn_iter_order = stream::poll_fn(|context| seq_rx.poll_recv(context))
                    .take(fn_graph.node_count())
                    .collect::<Vec<&'static str>>()
                    .await;

                assert_eq!(["f", "a", "c", "b", "d", "e"], fn_iter_order.as_slice());
                Ok(())
            })
        }

        #[test]
        fn fold_async_runs_fns_in_dep_order_concurrently_mut()
        -> Result<(), Box<dyn std::error::Error>> {
            let rt = runtime::Builder::new_current_thread()
                .enable_time()
                .build()?;
            rt.block_on(async {
                let (mut fn_graph, mut seq_rx) = complex_graph_unit_mut()?;

                let mut resources = Resources::new();
                resources.insert(0u8);
                resources.insert(0u16);
                fn_graph
                    .fold_async(resources, |resources, f| {
                        Box::pin(async move {
                            f.call_mut(&resources).await;
                            resources
                        })
                    })
                    .await;

                let fn_iter_order = stream::poll_fn(|context| seq_rx.poll_recv(context))
                    .collect::<Vec<&'static str>>()
                    .await;

                assert_eq!(["f", "a", "c", "b", "d", "e"], fn_iter_order.as_slice());
                Ok(())
            })
        }

        #[test]
        fn for_each_concurrent_runs_fns_concurrently_mut() -> Result<(), Box<dyn std::error::Error>>
        {
            let rt = runtime::Builder::new_current_thread()
                .enable_time()
                .build()?;
            rt.block_on(async {
                let (mut fn_graph, mut seq_rx) = complex_graph_unit_mut()?;

                let mut resources = Resources::new();
                resources.insert(0u8);
                resources.insert(0u16);
                let resources = &resources;
                fn_graph
                    .for_each_concurrent(None, |f| f.call_mut(resources).boxed())
                    .await;

                let fn_iter_order = stream::poll_fn(|context| seq_rx.poll_recv(context))
                    .collect::<Vec<&'static str>>()
                    .await;

                assert_eq!(["f", "a", "c", "b", "d", "e"], fn_iter_order.as_slice());
                Ok(())
            })
        }

        fn complex_graph_unit() -> Result<
            (
                FnGraph<Box<dyn FnRes<Ret = BoxFuture<'static, ()>>>>,
                Receiver<&'static str>,
            ),
            WouldCycle<Edge>,
        > {
            // a - b --------- e
            //   \          / /
            //    '-- c - d  /
            //              /
            //   f --------'
            //
            // `b`, `d`, and `f` all require `&mut u16`
            //
            // Data edges augmented for `b -> d`,`f -> b`

            let (seq_tx, seq_rx) = mpsc::channel(6);
            // Wrap in `Option` so that they can be dropped after one invocation, and thus
            // close the channel.
            let seq_tx_a = seq_tx.clone();
            let seq_tx_b = seq_tx.clone();
            let seq_tx_c = seq_tx.clone();
            let seq_tx_d = seq_tx.clone();
            let seq_tx_e = seq_tx.clone();
            let seq_tx_f = seq_tx;

            let mut fn_graph_builder = FnGraphBuilder::new();
            let [fn_id_a, fn_id_b, fn_id_c, fn_id_d, fn_id_e, fn_id_f] =
                fn_graph_builder.add_fns([
                    (move |_: &u8| -> BoxFuture<'_, ()> {
                        seq_tx_a
                            .try_send("a")
                            .expect("Failed to send sequence `a`.");
                        async {
                            time::sleep(Duration::from_millis(0)).await;
                        }
                        .boxed()
                    })
                    .into_fn_res(),
                    (move |_: &mut u16| -> BoxFuture<'_, ()> {
                        seq_tx_b
                            .try_send("b")
                            .expect("Failed to send sequence `b`.");
                        async {
                            time::sleep(Duration::from_millis(0)).await;
                        }
                        .boxed()
                    })
                    .into_fn_res(),
                    (move || -> BoxFuture<'_, ()> {
                        seq_tx_c
                            .try_send("c")
                            .expect("Failed to send sequence `c`.");
                        async {
                            time::sleep(Duration::from_millis(0)).await;
                        }
                        .boxed()
                    })
                    .into_fn_res(),
                    (move |_: &u8, _: &mut u16| -> BoxFuture<'_, ()> {
                        seq_tx_d
                            .try_send("d")
                            .expect("Failed to send sequence `d`.");
                        async {
                            time::sleep(Duration::from_millis(0)).await;
                        }
                        .boxed()
                    })
                    .into_fn_res(),
                    (move || -> BoxFuture<'_, ()> {
                        seq_tx_e
                            .try_send("e")
                            .expect("Failed to send sequence `e`.");
                        async {
                            time::sleep(Duration::from_millis(0)).await;
                        }
                        .boxed()
                    })
                    .into_fn_res(),
                    (move |_: &mut u16| -> BoxFuture<'_, ()> {
                        seq_tx_f
                            .try_send("f")
                            .expect("Failed to send sequence `f`.");
                        async {
                            time::sleep(Duration::from_millis(0)).await;
                        }
                        .boxed()
                    })
                    .into_fn_res(),
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
            Ok((fn_graph, seq_rx))
        }

        fn complex_graph_unit_mut() -> Result<
            (
                FnGraph<Box<dyn FnResMut<Ret = BoxFuture<'static, ()>>>>,
                Receiver<&'static str>,
            ),
            WouldCycle<Edge>,
        > {
            // a - b --------- e
            //   \          / /
            //    '-- c - d  /
            //              /
            //   f --------'
            //
            // `b`, `d`, and `f` all require `&mut u16`
            //
            // Data edges augmented for `b -> d`,`f -> b`

            let (seq_tx, seq_rx) = mpsc::channel(6);
            // Wrap in `Option` so that they can be dropped after one invocation, and thus
            // close the channel.
            let mut seq_tx_a = Some(seq_tx.clone());
            let mut seq_tx_b = Some(seq_tx.clone());
            let mut seq_tx_c = Some(seq_tx.clone());
            let mut seq_tx_d = Some(seq_tx.clone());
            let mut seq_tx_e = Some(seq_tx.clone());
            let mut seq_tx_f = Some(seq_tx);

            let mut fn_graph_builder = FnGraphBuilder::new();
            let [fn_id_a, fn_id_b, fn_id_c, fn_id_d, fn_id_e, fn_id_f] =
                fn_graph_builder.add_fns([
                    (move |_: &u8| -> BoxFuture<()> {
                        if let Some(seq_tx_a) = seq_tx_a.take() {
                            seq_tx_a
                                .try_send("a")
                                .expect("Failed to send sequence `a`.");
                        }
                        Box::pin(async {
                            time::sleep(Duration::from_millis(0)).await;
                        })
                    })
                    .into_fn_res_mut(),
                    (move |_: &mut u16| -> BoxFuture<()> {
                        if let Some(seq_tx_b) = seq_tx_b.take() {
                            seq_tx_b
                                .try_send("b")
                                .expect("Failed to send sequence `b`.");
                        }
                        Box::pin(async {
                            time::sleep(Duration::from_millis(0)).await;
                        })
                    })
                    .into_fn_res_mut(),
                    (move || -> BoxFuture<()> {
                        if let Some(seq_tx_c) = seq_tx_c.take() {
                            seq_tx_c
                                .try_send("c")
                                .expect("Failed to send sequence `c`.");
                        }
                        Box::pin(async {
                            time::sleep(Duration::from_millis(0)).await;
                        })
                    })
                    .into_fn_res_mut(),
                    (move |_: &u8, _: &mut u16| -> BoxFuture<()> {
                        if let Some(seq_tx_d) = seq_tx_d.take() {
                            seq_tx_d
                                .try_send("d")
                                .expect("Failed to send sequence `d`.");
                        }
                        Box::pin(async {
                            time::sleep(Duration::from_millis(0)).await;
                        })
                    })
                    .into_fn_res_mut(),
                    (move || -> BoxFuture<()> {
                        if let Some(seq_tx_e) = seq_tx_e.take() {
                            seq_tx_e
                                .try_send("e")
                                .expect("Failed to send sequence `e`.");
                        }
                        Box::pin(async {
                            time::sleep(Duration::from_millis(0)).await;
                        })
                    })
                    .into_fn_res_mut(),
                    (move |_: &mut u16| -> BoxFuture<()> {
                        if let Some(seq_tx_f) = seq_tx_f.take() {
                            seq_tx_f
                                .try_send("f")
                                .expect("Failed to send sequence `f`.");
                        }
                        Box::pin(async {
                            time::sleep(Duration::from_millis(0)).await;
                        })
                    })
                    .into_fn_res_mut(),
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
            Ok((fn_graph, seq_rx))
        }
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
