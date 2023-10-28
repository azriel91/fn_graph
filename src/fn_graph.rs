use std::{
    fmt::Debug,
    ops::{ControlFlow, Deref, DerefMut},
};

use daggy::{
    petgraph::{graph::NodeReferences, visit::Topo},
    Dag, NodeWeightsMut, Walker,
};
use fixedbitset::FixedBitSet;

use crate::{Edge, FnId, FnIdInner, Rank};

#[cfg(feature = "async")]
use daggy::NodeIndex;
#[cfg(feature = "async")]
use futures::{
    future::{Future, LocalBoxFuture},
    stream::{self, Stream, StreamExt},
    task::Poll,
};
#[cfg(feature = "async")]
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    RwLock,
};

#[cfg(feature = "async")]
use crate::{
    EdgeCounts, FnRef, StreamOpts, StreamOrder, StreamOutcome, StreamOutcomeState, StreamProgress,
    StreamProgressState,
};
#[cfg(all(feature = "async", feature = "interruptible"))]
use interruptible::{
    interrupt_strategy::{FinishCurrent, PollNextN},
    interruptibility::Interruptibility,
    InterruptStrategy, InterruptStrategyT, InterruptibleStream, InterruptibleStreamExt,
    StreamOutcome as InterruptibleStreamOutcome, StreamOutcomeNRemaining,
};

/// Directed acyclic graph of functions.
///
/// Besides the `iter_insertion*` function, all iteration functions run using
/// topological ordering -- where each function is ordered before its
/// successors.
#[derive(Clone, Debug)]
pub struct FnGraph<F> {
    /// Graph of functions.
    pub graph: Dag<F, Edge, FnIdInner>,
    /// Structure of the graph, with `()` as the node weights.
    pub(crate) graph_structure: Dag<(), Edge, FnIdInner>,
    /// Reversed structure of the graph, with `()` as the node weights.
    pub(crate) graph_structure_rev: Dag<(), Edge, FnIdInner>,
    /// Rank of each function.
    ///
    /// The `FnId` is guaranteed to match the index in the `Vec` as we never
    /// remove from the graph. So we don't need to use a `HashMap` here.
    pub(crate) ranks: Vec<Rank>,
    /// Number of incoming and outgoing edges of each function.
    #[cfg(feature = "async")]
    pub(crate) edge_counts: EdgeCounts,
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
        Topo::new(&self.graph_structure)
    }

    /// Returns an iterator of function references in topological order.
    ///
    /// Topological order is where each function is ordered before its
    /// successors.
    ///
    /// If you want to iterate over the functions in insertion order, see
    /// [`FnGraph::iter_insertion`].
    pub fn iter(&self) -> impl Iterator<Item = &F> {
        Topo::new(&self.graph_structure)
            .iter(&self.graph_structure)
            .map(|fn_id| &self.graph[fn_id])
    }

    /// Returns an iterator of function references in reverse topological order.
    ///
    /// Topological order is where each function is ordered before its
    /// successors, so this returns the function references from the leaf nodes
    /// to the root.
    ///
    /// If you want to iterate over the functions in reverse insertion order,
    /// use [`FnGraph::iter_insertion`], then call [`.rev()`].
    ///
    /// [`.rev()`]: std::iter::Iterator::rev
    pub fn iter_rev(&self) -> impl Iterator<Item = &F> {
        Topo::new(&self.graph_structure_rev)
            .iter(&self.graph_structure_rev)
            .map(|fn_id| &self.graph[fn_id])
    }

    /// Returns a stream of function references in topological order.
    ///
    /// Functions are produced by the stream only when all of their predecessors
    /// have returned.
    #[cfg(feature = "async")]
    pub fn stream(&self) -> impl Stream<Item = FnRef<'_, F>> + '_ {
        self.stream_internal(StreamOpts::default())
    }

    /// Returns a stream of function references in reverse topological order.
    ///
    /// Functions are produced by the stream only when all of their predecessors
    /// have returned.
    #[cfg(feature = "async")]
    pub fn stream_rev(&self) -> impl Stream<Item = FnRef<'_, F>> + '_ {
        self.stream_internal(StreamOpts::default().rev())
    }

    #[cfg(feature = "async")]
    fn stream_internal<'f>(
        &'f self,
        opts: StreamOpts<'f>,
    ) -> impl Stream<Item = FnRef<'f, F>> + 'f {
        let FnGraph {
            ref graph,
            ref graph_structure,
            ref graph_structure_rev,
            ranks: _,
            ref edge_counts,
        } = self;

        let StreamOpts {
            stream_order,
            // Currently we don't support interruptibility for `stream_internal` as this alters the
            // return type of the stream.
            #[cfg(feature = "interruptible")]
                interruptibility: _,
            marker: _,
        } = opts;

        let StreamSetupInit {
            graph_structure,
            mut predecessor_counts,
            fn_ready_tx,
            mut fn_ready_rx,
            fn_done_tx,
            mut fn_done_rx,
        } = stream_setup_init(
            graph_structure,
            graph_structure_rev,
            edge_counts,
            stream_order,
        );

        let mut fns_remaining = graph_structure.node_count();
        let mut fn_ready_tx = Some(fn_ready_tx);
        let mut fn_done_tx = Some(fn_done_tx);

        if fns_remaining == 0 {
            fn_done_tx.take();
            fn_ready_tx.take();
        }

        stream::poll_fn(move |context| {
            match fn_done_rx.poll_recv(context) {
                Poll::Pending => {}
                Poll::Ready(None) => {}
                Poll::Ready(Some(fn_id)) => graph_structure
                    .children(fn_id)
                    .iter(graph_structure)
                    .for_each(|(_edge_id, child_fn_id)| {
                        predecessor_counts[child_fn_id.index()] -= 1;
                        if predecessor_counts[child_fn_id.index()] == 0 {
                            if let Some(fn_ready_tx) = fn_ready_tx.as_ref() {
                                fn_ready_tx.try_send(child_fn_id).unwrap_or_else(
                                    #[cfg_attr(coverage_nightly, coverage(off))]
                                    |e| {
                                        panic!(
                                            "Failed to queue function `{}`. Cause: {}",
                                            fn_id.index(),
                                            e
                                        )
                                    },
                                );
                            }
                        }
                    }),
            }

            let poll = if let Some(fn_done_tx) = fn_done_tx.as_ref() {
                fn_ready_rx.poll_recv(context).map(|fn_id| {
                    fn_id.map(|fn_id| {
                        let r#fn = &graph[fn_id];
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
    pub async fn fold_async<Seed, FnFold>(
        &mut self,
        seed: Seed,
        fn_fold: FnFold,
    ) -> StreamOutcome<Option<Seed>>
    where
        FnFold: FnMut(Seed, &mut F) -> LocalBoxFuture<'_, Seed>,
    {
        self.fold_async_internal(seed, StreamOpts::default(), fn_fold)
            .await
    }

    /// Returns a stream of function references in topological order.
    ///
    /// Functions are produced by the stream only when all of their predecessors
    /// have returned.
    #[cfg(feature = "async")]
    pub async fn fold_async_with<'f, Seed, FnFold>(
        &mut self,
        seed: Seed,
        opts: StreamOpts<'f>,
        fn_fold: FnFold,
    ) -> StreamOutcome<Option<Seed>>
    where
        FnFold: FnMut(Seed, &mut F) -> LocalBoxFuture<'_, Seed>,
    {
        self.fold_async_internal(seed, opts, fn_fold).await
    }

    #[cfg(feature = "async")]
    async fn fold_async_internal<'f, Seed, FnFold>(
        &'f mut self,
        seed: Seed,
        opts: StreamOpts<'f>,
        fn_fold: FnFold,
    ) -> StreamOutcome<Option<Seed>>
    where
        FnFold: FnMut(Seed, &mut F) -> LocalBoxFuture<'_, Seed>,
    {
        let FnGraph {
            ref mut graph,
            ref graph_structure,
            ref graph_structure_rev,
            ranks: _,
            ref edge_counts,
        } = self;

        let StreamOpts {
            stream_order,
            #[cfg(feature = "interruptible")]
            interruptibility,
            marker: _,
        } = opts;

        let StreamSetupInit {
            graph_structure,
            predecessor_counts,
            fn_ready_tx,
            mut fn_ready_rx,
            fn_done_tx,
            fn_done_rx,
        } = stream_setup_init(
            graph_structure,
            graph_structure_rev,
            edge_counts,
            stream_order,
        );

        let queuer = fn_ready_queuer(
            graph_structure,
            predecessor_counts,
            fn_done_rx,
            fn_ready_tx,
            #[cfg(feature = "interruptible")]
            interruptibility,
        );

        let fns_remaining = graph_structure.node_count();
        let mut fn_done_tx = Some(fn_done_tx);
        if fns_remaining == 0 {
            fn_done_tx.take();
        }
        let fold_stream_state = FoldStreamState {
            graph,
            fns_remaining,
            fn_done_tx,
            seed,
            fn_fold,
        };
        let scheduler = async move {
            let fold_stream_state = stream::poll_fn(move |context| fn_ready_rx.poll_recv(context))
                .fold(fold_stream_state, |fold_stream_state, fn_id| async move {
                    let FoldStreamState {
                        graph,
                        mut fns_remaining,
                        mut fn_done_tx,
                        seed,
                        mut fn_fold,
                    } = fold_stream_state;
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

                    FoldStreamState {
                        graph,
                        fns_remaining,
                        fn_done_tx,
                        seed,
                        fn_fold,
                    }
                })
                .await;

            let FoldStreamState {
                graph: _,
                fns_remaining: _,
                fn_done_tx: _,
                seed,
                fn_fold: _,
            } = fold_stream_state;

            seed
        };

        let (fn_graph_stream_outcome, seed) = futures::join!(queuer, scheduler);

        fn_graph_stream_outcome.map(|()| Some(seed))
    }

    /// Runs the provided logic over the functions concurrently in topological
    /// order.
    ///
    /// The first argument is an optional `limit` on the number of concurrent
    /// futures. If this limit is not `None`, no more than `limit` futures will
    /// be run concurrently. The `limit` argument is of type
    /// `Into<Option<usize>>`, and so can be provided as either `None`,
    /// `Some(10)`, or just `10`.
    ///
    /// **Note:** a limit of zero is interpreted as no limit at all, and will
    /// have the same result as passing in `None`.
    #[cfg(feature = "async")]
    pub async fn for_each_concurrent<'f, FnForEach, Fut>(
        &'f self,
        limit: impl Into<Option<usize>>,
        fn_for_each: FnForEach,
    ) -> StreamOutcome<()>
    where
        FnForEach: Fn(&'f F) -> Fut,
        Fut: Future<Output = ()> + 'f,
        F: 'f,
    {
        self.for_each_concurrent_internal(limit, StreamOpts::default(), fn_for_each)
            .await
    }

    /// Runs the provided logic over the functions concurrently with the given
    /// stream options.
    ///
    /// The first argument is an optional `limit` on the number of concurrent
    /// futures. If this limit is not `None`, no more than `limit` futures will
    /// be run concurrently. The `limit` argument is of type
    /// `Into<Option<usize>>`, and so can be provided as either `None`,
    /// `Some(10)`, or just `10`.
    ///
    /// **Note:** a limit of zero is interpreted as no limit at all, and will
    /// have the same result as passing in `None`.
    #[cfg(feature = "async")]
    pub async fn for_each_concurrent_with<'f, FnForEach, Fut>(
        &'f self,
        limit: impl Into<Option<usize>>,
        opts: StreamOpts<'f>,
        fn_for_each: FnForEach,
    ) -> StreamOutcome<()>
    where
        FnForEach: Fn(&'f F) -> Fut,
        Fut: Future<Output = ()> + 'f,
        F: 'f,
    {
        self.for_each_concurrent_internal(limit, opts, fn_for_each)
            .await
    }

    // https://users.rust-lang.org/t/lifetime-may-not-live-long-enough-for-an-async-closure/62489
    #[cfg(feature = "async")]
    async fn for_each_concurrent_internal<'f, FnForEach, Fut>(
        &'f self,
        limit: impl Into<Option<usize>>,
        opts: StreamOpts<'f>,
        fn_for_each: FnForEach,
    ) -> StreamOutcome<()>
    where
        FnForEach: Fn(&'f F) -> Fut,
        Fut: Future<Output = ()> + 'f,
        F: 'f,
    {
        let FnGraph {
            ref graph,
            ref graph_structure,
            ref graph_structure_rev,
            ranks: _,
            ref edge_counts,
        } = self;

        let StreamSetupInitConcurrent {
            graph_structure,
            mut fn_ready_rx,
            queuer,
            fn_done_tx,
            fns_remaining,
        } = stream_setup_init_concurrent(graph_structure, graph_structure_rev, edge_counts, opts);

        let fn_done_tx = &fn_done_tx;
        let fn_for_each = &fn_for_each;
        let fns_remaining = &fns_remaining;
        let fn_refs = graph;

        if graph_structure.node_count() == 0 {
            fn_done_tx.write().await.take();
        }
        let scheduler = async move {
            stream::poll_fn(move |context| fn_ready_rx.poll_recv(context))
                .for_each_concurrent(limit, |fn_id| async move {
                    let r#fn = fn_refs.node_weight(fn_id).expect("Expected to borrow fn.");
                    fn_for_each(r#fn).await;
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

        let (fn_graph_stream_outcome, ()) = futures::join!(queuer, scheduler);
        fn_graph_stream_outcome
    }

    /// Runs the provided logic over the functions concurrently in topological
    /// order.
    ///
    /// The first argument is an optional `limit` on the number of concurrent
    /// futures. If this limit is not `None`, no more than `limit` futures will
    /// be run concurrently. The `limit` argument is of type
    /// `Into<Option<usize>>`, and so can be provided as either `None`,
    /// `Some(10)`, or just `10`.
    ///
    /// **Note:** a limit of zero is interpreted as no limit at all, and will
    /// have the same result as passing in `None`.
    #[cfg(feature = "async")]
    pub async fn for_each_concurrent_mut<FnForEach, Fut>(
        &mut self,
        limit: impl Into<Option<usize>>,
        fn_for_each: FnForEach,
    ) -> StreamOutcome<()>
    where
        FnForEach: Fn(&mut F) -> Fut,
        Fut: Future<Output = ()>,
    {
        self.for_each_concurrent_mut_internal(limit, StreamOpts::default(), fn_for_each)
            .await
    }

    /// Runs the provided logic over the functions concurrently in reverse
    /// topological order.
    ///
    /// The first argument is an optional `limit` on the number of concurrent
    /// futures. If this limit is not `None`, no more than `limit` futures will
    /// be run concurrently. The `limit` argument is of type
    /// `Into<Option<usize>>`, and so can be provided as either `None`,
    /// `Some(10)`, or just `10`.
    ///
    /// **Note:** a limit of zero is interpreted as no limit at all, and will
    /// have the same result as passing in `None`.
    #[cfg(feature = "async")]
    pub async fn for_each_concurrent_mut_with<'f, FnForEach, Fut>(
        &mut self,
        limit: impl Into<Option<usize>>,
        opts: StreamOpts<'f>,
        fn_for_each: FnForEach,
    ) -> StreamOutcome<()>
    where
        FnForEach: Fn(&mut F) -> Fut,
        Fut: Future<Output = ()>,
    {
        self.for_each_concurrent_mut_internal(limit, opts, fn_for_each)
            .await
    }

    // https://users.rust-lang.org/t/lifetime-may-not-live-long-enough-for-an-async-closure/62489
    #[cfg(feature = "async")]
    async fn for_each_concurrent_mut_internal<'f, FnForEach, Fut>(
        &mut self,
        limit: impl Into<Option<usize>>,
        opts: StreamOpts<'f>,
        fn_for_each: FnForEach,
    ) -> StreamOutcome<()>
    where
        FnForEach: Fn(&mut F) -> Fut,
        Fut: Future<Output = ()>,
    {
        let FnGraph {
            ref mut graph,
            ref graph_structure,
            ref graph_structure_rev,
            ranks: _,
            ref edge_counts,
        } = self;

        let StreamSetupInitConcurrent {
            graph_structure,
            mut fn_ready_rx,
            queuer,
            fn_done_tx,
            fns_remaining,
        } = stream_setup_init_concurrent(graph_structure, graph_structure_rev, edge_counts, opts);

        let fn_done_tx = &fn_done_tx;
        let fn_for_each = &fn_for_each;
        let fns_remaining = &fns_remaining;
        let fn_mut_refs = graph
            .node_weights_mut()
            .map(RwLock::new)
            .collect::<Vec<_>>();
        let fn_mut_refs = &fn_mut_refs;

        if graph_structure.node_count() == 0 {
            fn_done_tx.write().await.take();
        }
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

        let (fn_graph_stream_outcome, ()) = futures::join!(queuer, scheduler);
        fn_graph_stream_outcome
    }

    /// Runs the provided logic over the functions concurrently in topological
    /// order, stopping when an error is encountered.
    ///
    /// This gracefully waits until all produced tasks have returned. The return
    /// error type is a `Vec<E>` as it is possible for multiple tasks to return
    /// errors.
    ///
    /// The first argument is an optional `limit` on the number of concurrent
    /// futures. If this limit is not `None`, no more than `limit` futures will
    /// be run concurrently. The `limit` argument is of type
    /// `Into<Option<usize>>`, and so can be provided as either `None`,
    /// `Some(10)`, or just `10`.
    ///
    /// **Note:** a limit of zero is interpreted as no limit at all, and will
    /// have the same result as passing in `None`.
    #[cfg(feature = "async")]
    pub async fn try_for_each_concurrent<'f, E, FnTryForEach, Fut>(
        &'f self,
        limit: impl Into<Option<usize>>,
        fn_try_for_each: FnTryForEach,
    ) -> Result<StreamOutcome<()>, (StreamOutcome<()>, Vec<E>)>
    where
        E: Debug,
        FnTryForEach: Fn(&'f F) -> Fut,
        Fut: Future<Output = Result<(), E>> + 'f,
    {
        self.try_for_each_concurrent_internal(limit, StreamOpts::default(), fn_try_for_each)
            .await
    }

    /// Runs the provided logic over the functions concurrently with the given
    /// options, stopping when an error is encountered.
    ///
    /// This gracefully waits until all produced tasks have returned. The return
    /// error type is a `Vec<E>` as it is possible for multiple tasks to return
    /// errors.
    ///
    /// The first argument is an optional `limit` on the number of concurrent
    /// futures. If this limit is not `None`, no more than `limit` futures will
    /// be run concurrently. The `limit` argument is of type
    /// `Into<Option<usize>>`, and so can be provided as either `None`,
    /// `Some(10)`, or just `10`.
    ///
    /// **Note:** a limit of zero is interpreted as no limit at all, and will
    /// have the same result as passing in `None`.
    #[cfg(feature = "async")]
    pub async fn try_for_each_concurrent_with<'f, E, FnTryForEach, Fut>(
        &'f self,
        limit: impl Into<Option<usize>>,
        opts: StreamOpts<'f>,
        fn_try_for_each: FnTryForEach,
    ) -> Result<StreamOutcome<()>, (StreamOutcome<()>, Vec<E>)>
    where
        E: Debug,
        FnTryForEach: Fn(&'f F) -> Fut,
        Fut: Future<Output = Result<(), E>> + 'f,
    {
        self.try_for_each_concurrent_internal(limit, opts, fn_try_for_each)
            .await
    }

    /// Runs the provided logic over the functions concurrently in topological
    /// order, stopping when an error is encountered.
    ///
    /// This gracefully waits until all produced tasks have returned.
    ///
    /// The first argument is an optional `limit` on the number of concurrent
    /// futures. If this limit is not `None`, no more than `limit` futures will
    /// be run concurrently. The `limit` argument is of type
    /// `Into<Option<usize>>`, and so can be provided as either `None`,
    /// `Some(10)`, or just `10`.
    ///
    /// **Note:** a limit of zero is interpreted as no limit at all, and will
    /// have the same result as passing in `None`.
    #[cfg(feature = "async")]
    pub async fn try_for_each_concurrent_control<'f, E, FnTryForEach, Fut>(
        &'f self,
        limit: impl Into<Option<usize>>,
        fn_try_for_each: FnTryForEach,
    ) -> ControlFlow<(StreamOutcome<()>, Vec<E>), StreamOutcome<()>>
    where
        E: Debug,
        FnTryForEach: Fn(&'f F) -> Fut,
        Fut: Future<Output = ControlFlow<E, ()>> + 'f,
    {
        let result = self
            .try_for_each_concurrent_internal(limit, StreamOpts::default(), |f| {
                let fut = fn_try_for_each(f);
                async move {
                    match fut.await {
                        ControlFlow::Continue(()) => Result::Ok(()),
                        ControlFlow::Break(e) => Result::Err(e),
                    }
                }
            })
            .await;
        match result {
            Result::Ok(outcome) => match outcome.state {
                StreamOutcomeState::NotStarted | StreamOutcomeState::Interrupted => {
                    ControlFlow::Break((outcome, Vec::new()))
                }
                StreamOutcomeState::Finished => ControlFlow::Continue(outcome),
            },
            Result::Err(outcome_and_err) => ControlFlow::Break(outcome_and_err),
        }
    }

    /// Runs the provided logic over the functions concurrently with the given
    /// options, stopping when an error is encountered.
    ///
    /// This gracefully waits until all produced tasks have returned.
    ///
    /// The first argument is an optional `limit` on the number of concurrent
    /// futures. If this limit is not `None`, no more than `limit` futures will
    /// be run concurrently. The `limit` argument is of type
    /// `Into<Option<usize>>`, and so can be provided as either `None`,
    /// `Some(10)`, or just `10`.
    ///
    /// **Note:** a limit of zero is interpreted as no limit at all, and will
    /// have the same result as passing in `None`.
    #[cfg(feature = "async")]
    pub async fn try_for_each_concurrent_control_with<'f, E, FnTryForEach, Fut>(
        &'f self,
        limit: impl Into<Option<usize>>,
        opts: StreamOpts<'f>,
        fn_try_for_each: FnTryForEach,
    ) -> ControlFlow<(StreamOutcome<()>, Vec<E>), StreamOutcome<()>>
    where
        E: Debug,
        FnTryForEach: Fn(&'f F) -> Fut,
        Fut: Future<Output = ControlFlow<E, ()>> + 'f,
    {
        let result = self
            .try_for_each_concurrent_internal(limit, opts, |f| {
                let fut = fn_try_for_each(f);
                async move {
                    match fut.await {
                        ControlFlow::Continue(()) => Result::Ok(()),
                        ControlFlow::Break(e) => Result::Err(e),
                    }
                }
            })
            .await;
        match result {
            Result::Ok(outcome) => match outcome.state {
                StreamOutcomeState::NotStarted | StreamOutcomeState::Interrupted => {
                    ControlFlow::Break((outcome, Vec::new()))
                }
                StreamOutcomeState::Finished => ControlFlow::Continue(outcome),
            },
            Result::Err(outcome_and_err) => ControlFlow::Break(outcome_and_err),
        }
    }

    // https://users.rust-lang.org/t/lifetime-may-not-live-long-enough-for-an-async-closure/62489
    #[cfg(feature = "async")]
    async fn try_for_each_concurrent_internal<'f, E, FnTryForEach, Fut>(
        &'f self,
        limit: impl Into<Option<usize>>,
        opts: StreamOpts<'f>,
        fn_try_for_each: FnTryForEach,
    ) -> Result<StreamOutcome<()>, (StreamOutcome<()>, Vec<E>)>
    where
        E: Debug,
        FnTryForEach: Fn(&'f F) -> Fut,
        Fut: Future<Output = Result<(), E>> + 'f,
    {
        let StreamOpts {
            stream_order,
            #[cfg(feature = "interruptible")]
            interruptibility,
            marker: _,
        } = opts;

        let (graph_structure, predecessor_counts) = match stream_order {
            StreamOrder::Forward => (&self.graph_structure, self.edge_counts.incoming().to_vec()),
            StreamOrder::Reverse => (
                &self.graph_structure_rev,
                self.edge_counts.outgoing().to_vec(),
            ),
        };
        let channel_capacity = std::cmp::max(1, graph_structure.node_count());
        let (result_tx, mut result_rx) = mpsc::channel(channel_capacity);
        let (fn_ready_tx, mut fn_ready_rx) = mpsc::channel(channel_capacity);
        let (fn_done_tx, fn_done_rx) = mpsc::channel::<FnId>(channel_capacity);

        // Preload the channel with all of the functions that have no predecessors
        Topo::new(graph_structure)
            .iter(graph_structure)
            .filter(|fn_id| predecessor_counts[fn_id.index()] == 0)
            .try_for_each(|fn_id| fn_ready_tx.try_send(fn_id))
            .expect("Failed to preload function with no predecessors.");

        let queuer = fn_ready_queuer(
            graph_structure,
            predecessor_counts,
            fn_done_rx,
            fn_ready_tx,
            #[cfg(feature = "interruptible")]
            interruptibility,
        );

        let fn_done_tx = RwLock::new(Some(fn_done_tx));
        let fn_done_tx = &fn_done_tx;
        let fn_try_for_each = &fn_try_for_each;
        let fns_remaining = graph_structure.node_count();
        let fns_remaining = RwLock::new(fns_remaining);
        let fns_remaining = &fns_remaining;
        let fn_refs = &self.graph;

        if graph_structure.node_count() == 0 {
            fn_done_tx.write().await.take();
        }
        let scheduler = async move {
            let result_tx_ref = &result_tx;

            stream::poll_fn(move |context| fn_ready_rx.poll_recv(context))
                .for_each_concurrent(limit, |fn_id| async move {
                    let r#fn = fn_refs.node_weight(fn_id).expect("Expected to borrow fn.");
                    if let Err(e) = fn_try_for_each(r#fn).await {
                        result_tx_ref
                            .send(e)
                            .await
                            .expect("Scheduler failed to send Err result in `result_tx`.");

                        // Close `fn_done_rx`, which means `fn_ready_queuer` should return
                        // `Poll::Ready(None)`.
                        fn_done_tx.write().await.take();
                    };

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

            drop(result_tx);
        };

        let (fn_graph_stream_outcome, ()) = futures::join!(queuer, scheduler);

        let results = stream::poll_fn(move |ctx| result_rx.poll_recv(ctx))
            .collect::<Vec<E>>()
            .await;

        if results.is_empty() {
            Ok(fn_graph_stream_outcome)
        } else {
            Err((fn_graph_stream_outcome, results))
        }
    }

    /// Runs the provided logic over the functions concurrently in topological
    /// order, stopping when an error is encountered.
    ///
    /// This gracefully waits until all produced tasks have returned. The return
    /// error type is a `Vec<E>` as it is possible for multiple tasks to return
    /// errors.
    ///
    /// The first argument is an optional `limit` on the number of concurrent
    /// futures. If this limit is not `None`, no more than `limit` futures will
    /// be run concurrently. The `limit` argument is of type
    /// `Into<Option<usize>>`, and so can be provided as either `None`,
    /// `Some(10)`, or just `10`.
    ///
    /// **Note:** a limit of zero is interpreted as no limit at all, and will
    /// have the same result as passing in `None`.
    #[cfg(feature = "async")]
    pub async fn try_for_each_concurrent_mut<E, FnTryForEach, Fut>(
        &mut self,
        limit: impl Into<Option<usize>>,
        fn_try_for_each: FnTryForEach,
    ) -> Result<StreamOutcome<()>, (StreamOutcome<()>, Vec<E>)>
    where
        E: Debug,
        FnTryForEach: Fn(&mut F) -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        self.try_for_each_concurrent_mut_internal(limit, StreamOpts::default(), fn_try_for_each)
            .await
    }

    /// Runs the provided logic over the functions concurrently in reverse
    /// topological order, stopping when an error is encountered.
    ///
    /// This gracefully waits until all produced tasks have returned. The return
    /// error type is a `Vec<E>` as it is possible for multiple tasks to return
    /// errors.
    ///
    /// The first argument is an optional `limit` on the number of concurrent
    /// futures. If this limit is not `None`, no more than `limit` futures will
    /// be run concurrently. The `limit` argument is of type
    /// `Into<Option<usize>>`, and so can be provided as either `None`,
    /// `Some(10)`, or just `10`.
    ///
    /// **Note:** a limit of zero is interpreted as no limit at all, and will
    /// have the same result as passing in `None`.
    #[cfg(feature = "async")]
    pub async fn try_for_each_concurrent_mut_with<'f, E, FnTryForEach, Fut>(
        &mut self,
        limit: impl Into<Option<usize>>,
        opts: StreamOpts<'f>,
        fn_try_for_each: FnTryForEach,
    ) -> Result<StreamOutcome<()>, (StreamOutcome<()>, Vec<E>)>
    where
        E: Debug,
        FnTryForEach: Fn(&mut F) -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        self.try_for_each_concurrent_mut_internal(limit, opts, fn_try_for_each)
            .await
    }

    /// Runs the provided logic over the functions concurrently in topological
    /// order, stopping when an error is encountered.
    ///
    /// This gracefully waits until all produced tasks have returned.
    ///
    /// The first argument is an optional `limit` on the number of concurrent
    /// futures. If this limit is not `None`, no more than `limit` futures will
    /// be run concurrently. The `limit` argument is of type
    /// `Into<Option<usize>>`, and so can be provided as either `None`,
    /// `Some(10)`, or just `10`.
    ///
    /// **Note:** a limit of zero is interpreted as no limit at all, and will
    /// have the same result as passing in `None`.
    #[cfg(feature = "async")]
    pub async fn try_for_each_concurrent_control_mut<E, FnTryForEach, Fut>(
        &mut self,
        limit: impl Into<Option<usize>>,
        fn_try_for_each: FnTryForEach,
    ) -> ControlFlow<(StreamOutcome<()>, Vec<E>), StreamOutcome<()>>
    where
        E: Debug,
        FnTryForEach: Fn(&mut F) -> Fut,
        Fut: Future<Output = ControlFlow<E, ()>>,
    {
        let result = self
            .try_for_each_concurrent_mut_internal(limit, StreamOpts::default(), |f| {
                let fut = fn_try_for_each(f);
                async move {
                    match fut.await {
                        ControlFlow::Continue(()) => Result::Ok(()),
                        ControlFlow::Break(e) => Result::Err(e),
                    }
                }
            })
            .await;
        match result {
            Result::Ok(outcome) => match outcome.state {
                StreamOutcomeState::NotStarted | StreamOutcomeState::Interrupted => {
                    ControlFlow::Break((outcome, Vec::new()))
                }
                StreamOutcomeState::Finished => ControlFlow::Continue(outcome),
            },
            Result::Err(outcome_and_err) => ControlFlow::Break(outcome_and_err),
        }
    }

    /// Runs the provided logic over the functions concurrently in topological
    /// order, stopping when an error is encountered.
    ///
    /// This gracefully waits until all produced tasks have returned.
    ///
    /// The first argument is an optional `limit` on the number of concurrent
    /// futures. If this limit is not `None`, no more than `limit` futures will
    /// be run concurrently. The `limit` argument is of type
    /// `Into<Option<usize>>`, and so can be provided as either `None`,
    /// `Some(10)`, or just `10`.
    ///
    /// **Note:** a limit of zero is interpreted as no limit at all, and will
    /// have the same result as passing in `None`.
    #[cfg(feature = "async")]
    pub async fn try_for_each_concurrent_control_mut_with<'f, E, FnTryForEach, Fut>(
        &mut self,
        limit: impl Into<Option<usize>>,
        opts: StreamOpts<'f>,
        fn_try_for_each: FnTryForEach,
    ) -> ControlFlow<(StreamOutcome<()>, Vec<E>), StreamOutcome<()>>
    where
        E: Debug,
        FnTryForEach: Fn(&mut F) -> Fut,
        Fut: Future<Output = ControlFlow<E, ()>>,
    {
        let result = self
            .try_for_each_concurrent_mut_internal(limit, opts, |f| {
                let fut = fn_try_for_each(f);
                async move {
                    match fut.await {
                        ControlFlow::Continue(()) => Result::Ok(()),
                        ControlFlow::Break(e) => Result::Err(e),
                    }
                }
            })
            .await;
        match result {
            Result::Ok(outcome) => match outcome.state {
                StreamOutcomeState::NotStarted | StreamOutcomeState::Interrupted => {
                    ControlFlow::Break((outcome, Vec::new()))
                }
                StreamOutcomeState::Finished => ControlFlow::Continue(outcome),
            },
            Result::Err(outcome_and_err) => ControlFlow::Break(outcome_and_err),
        }
    }

    // https://users.rust-lang.org/t/lifetime-may-not-live-long-enough-for-an-async-closure/62489
    #[cfg(feature = "async")]
    async fn try_for_each_concurrent_mut_internal<'f, E, FnTryForEach, Fut>(
        &mut self,
        limit: impl Into<Option<usize>>,
        opts: StreamOpts<'f>,
        fn_try_for_each: FnTryForEach,
    ) -> Result<StreamOutcome<()>, (StreamOutcome<()>, Vec<E>)>
    where
        E: Debug,
        FnTryForEach: Fn(&mut F) -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        let StreamOpts {
            stream_order,
            #[cfg(feature = "interruptible")]
            interruptibility,
            marker: _,
        } = opts;

        let (graph_structure, predecessor_counts) = match stream_order {
            StreamOrder::Forward => (&self.graph_structure, self.edge_counts.incoming().to_vec()),
            StreamOrder::Reverse => (
                &self.graph_structure_rev,
                self.edge_counts.outgoing().to_vec(),
            ),
        };
        let channel_capacity = std::cmp::max(1, graph_structure.node_count());
        let (result_tx, mut result_rx) = mpsc::channel(channel_capacity);
        let (fn_ready_tx, mut fn_ready_rx) = mpsc::channel(channel_capacity);
        let (fn_done_tx, fn_done_rx) = mpsc::channel::<FnId>(channel_capacity);

        // Preload the channel with all of the functions that have no predecessors
        Topo::new(graph_structure)
            .iter(graph_structure)
            .filter(|fn_id| predecessor_counts[fn_id.index()] == 0)
            .try_for_each(|fn_id| fn_ready_tx.try_send(fn_id))
            .expect("Failed to preload function with no predecessors.");

        let queuer = fn_ready_queuer(
            graph_structure,
            predecessor_counts,
            fn_done_rx,
            fn_ready_tx,
            #[cfg(feature = "interruptible")]
            interruptibility,
        );

        let fn_done_tx = RwLock::new(Some(fn_done_tx));
        let fn_done_tx = &fn_done_tx;
        let fn_try_for_each = &fn_try_for_each;
        let fns_remaining = graph_structure.node_count();
        let fns_remaining = RwLock::new(fns_remaining);
        let fns_remaining = &fns_remaining;
        let fn_mut_refs = self
            .graph
            .node_weights_mut()
            .map(RwLock::new)
            .collect::<Vec<_>>();
        let fn_mut_refs = &fn_mut_refs;
        let scheduler = async move {
            let result_tx_ref = &result_tx;

            stream::poll_fn(move |context| fn_ready_rx.poll_recv(context))
                .for_each_concurrent(limit, |fn_id| async move {
                    let mut r#fn = fn_mut_refs[fn_id.index()]
                        .try_write()
                        .expect("Expected to borrow fn mutably.");
                    if let Err(e) = fn_try_for_each(&mut r#fn).await {
                        result_tx_ref
                            .send(e)
                            .await
                            .expect("Scheduler failed to send Err result in `result_tx`.");

                        // Close `fn_done_rx`, which means `fn_ready_queuer` should return
                        // `Poll::Ready(None)`.
                        fn_done_tx.write().await.take();
                    };

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

            drop(result_tx);
        };

        let (fn_graph_stream_outcome, ()) = futures::join!(queuer, scheduler);

        let results = stream::poll_fn(move |ctx| result_rx.poll_recv(ctx))
            .collect::<Vec<E>>()
            .await;

        if results.is_empty() {
            Ok(fn_graph_stream_outcome)
        } else {
            Err((fn_graph_stream_outcome, results))
        }
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

#[cfg(feature = "async")]
fn stream_setup_init<'f>(
    graph_structure: &'f Dag<(), Edge, FnIdInner>,
    graph_structure_rev: &'f Dag<(), Edge, FnIdInner>,
    edge_counts: &EdgeCounts,
    stream_order: StreamOrder,
) -> StreamSetupInit<'f> {
    let (graph_structure, predecessor_counts) = match stream_order {
        StreamOrder::Forward => (graph_structure, edge_counts.incoming().to_vec()),
        StreamOrder::Reverse => (graph_structure_rev, edge_counts.outgoing().to_vec()),
    };
    // Decrement `predecessor_counts[child_fn_id]` every time a function ref
    // is dropped, and if `predecessor_counts[child_fn_id]` is 0, the stream
    // can produce `child_fn`s.
    let channel_capacity = std::cmp::max(1, graph_structure.node_count());
    let (fn_ready_tx, fn_ready_rx) = mpsc::channel(channel_capacity);
    let (fn_done_tx, fn_done_rx) = mpsc::channel::<FnId>(channel_capacity);

    // Preload the channel with all of the functions that have no predecessors
    Topo::new(&graph_structure)
        .iter(&graph_structure)
        .filter(|fn_id| predecessor_counts[fn_id.index()] == 0)
        .try_for_each(|fn_id| fn_ready_tx.try_send(fn_id))
        .expect("Failed to preload function with no predecessors.");

    StreamSetupInit {
        graph_structure,
        predecessor_counts,
        fn_ready_tx,
        fn_ready_rx,
        fn_done_tx,
        fn_done_rx,
    }
}

#[cfg(feature = "async")]
fn stream_setup_init_concurrent<'f>(
    graph_structure: &'f Dag<(), Edge, FnIdInner>,
    graph_structure_rev: &'f Dag<(), Edge, FnIdInner>,
    edge_counts: &EdgeCounts,
    opts: StreamOpts<'f>,
) -> StreamSetupInitConcurrent<'f, impl Future<Output = StreamOutcome<()>> + 'f> {
    let StreamOpts {
        stream_order,
        #[cfg(feature = "interruptible")]
        interruptibility,
        marker: _,
    } = opts;

    let StreamSetupInit {
        graph_structure,
        predecessor_counts,
        fn_ready_tx,
        fn_ready_rx,
        fn_done_tx,
        fn_done_rx,
    } = stream_setup_init(
        graph_structure,
        graph_structure_rev,
        edge_counts,
        stream_order,
    );

    let queuer = fn_ready_queuer(
        graph_structure,
        predecessor_counts,
        fn_done_rx,
        fn_ready_tx,
        #[cfg(feature = "interruptible")]
        interruptibility,
    );

    let fn_done_tx = RwLock::new(Some(fn_done_tx));
    let fns_remaining = graph_structure.node_count();
    let fns_remaining = RwLock::new(fns_remaining);
    StreamSetupInitConcurrent {
        graph_structure,
        fn_ready_rx,
        queuer,
        fn_done_tx,
        fns_remaining,
    }
}

/// Sends IDs of function whose predecessors have been executed to
/// `fn_ready_tx`.
#[cfg(feature = "async")]
async fn fn_ready_queuer<'f>(
    graph_structure: &Dag<(), Edge, FnIdInner>,
    predecessor_counts: Vec<usize>,
    mut fn_done_rx: Receiver<FnId>,
    fn_ready_tx: Sender<FnId>,
    #[cfg(feature = "interruptible")] interruptibility: Interruptibility<'f>,
) -> StreamOutcome<()> {
    let fns_remaining = graph_structure.node_count();
    let mut fn_graph_stream_progress = StreamProgress::with_capacity((), fns_remaining);

    let mut fn_ready_tx = Some(fn_ready_tx);
    if fns_remaining == 0 {
        fn_graph_stream_progress.state = StreamProgressState::Finished;
        fn_ready_tx.take();
    }
    let stream = stream::poll_fn(move |context| fn_done_rx.poll_recv(context));

    #[cfg(not(feature = "interruptible"))]
    {
        let queuer_stream_state =
            QueuerStreamState::new(fns_remaining, predecessor_counts, fn_ready_tx);
        queuer_stream_fold(stream, queuer_stream_state, graph_structure).await
    }

    #[cfg(feature = "interruptible")]
    match interruptibility {
        Interruptibility::NonInterruptible => {
            let queuer_stream_state =
                QueuerStreamState::new(fns_remaining, predecessor_counts, fn_ready_tx);
            queuer_stream_fold(stream, queuer_stream_state, graph_structure).await
        }
        Interruptibility::Interruptible {
            interrupt_rx,
            interrupt_strategy,
        } => match interrupt_strategy {
            InterruptStrategy::FinishCurrent => {
                let queuer_stream_state_interruptible = QueuerStreamStateInterruptible {
                    fns_remaining,
                    predecessor_counts,
                    fn_ready_tx,
                    fn_graph_stream_progress,
                };
                queuer_stream_fold_interruptible::<
                    InterruptibleStreamOutcome<NodeIndex<FnIdInner>>,
                    _,
                    FinishCurrent,
                >(
                    stream.interruptible_with(interrupt_rx, FinishCurrent),
                    queuer_stream_state_interruptible,
                    graph_structure,
                    |stream_outcome| match stream_outcome {
                        InterruptibleStreamOutcome::InterruptBeforePoll => (None, true),
                        InterruptibleStreamOutcome::InterruptDuringPoll(fn_id) => {
                            (Some(fn_id), true)
                        }
                        InterruptibleStreamOutcome::NoInterrupt(fn_id) => (Some(fn_id), false),
                    },
                )
                .await
            }
            InterruptStrategy::PollNextN(n) => {
                let queuer_stream_state_interruptible = QueuerStreamStateInterruptible {
                    fns_remaining,
                    predecessor_counts,
                    fn_ready_tx,
                    fn_graph_stream_progress,
                };
                queuer_stream_fold_interruptible::<
                    StreamOutcomeNRemaining<NodeIndex<FnIdInner>>,
                    _,
                    PollNextN,
                >(
                    stream.interruptible_with(interrupt_rx, PollNextN(n)),
                    queuer_stream_state_interruptible,
                    graph_structure,
                    |stream_outcome_n_remaining| match stream_outcome_n_remaining {
                        StreamOutcomeNRemaining::InterruptBeforePoll => (None, true),
                        StreamOutcomeNRemaining::InterruptDuringPoll {
                            value: fn_id,
                            n_remaining: _,
                        } => (Some(fn_id), true),
                        StreamOutcomeNRemaining::NoInterrupt(fn_id) => (Some(fn_id), false),
                    },
                )
                .await
            }
        },
    }
}

/// Polls the queuer stream until completed.
#[cfg(feature = "async")]
async fn queuer_stream_fold(
    stream: stream::PollFn<
        impl FnMut(&mut std::task::Context) -> Poll<Option<NodeIndex<FnIdInner>>>,
    >,
    queuer_stream_state: QueuerStreamState,
    graph_structure: &Dag<(), Edge, FnIdInner>,
) -> StreamOutcome<()> {
    let queuer_stream_state = stream
        .fold(
            queuer_stream_state,
            move |queuer_stream_state, fn_id| async move {
                let QueuerStreamState {
                    mut fns_remaining,
                    mut predecessor_counts,
                    mut fn_ready_tx,
                    mut fn_ids_processed,
                } = queuer_stream_state;

                // Close `fn_ready_rx` when all functions have been executed,
                fns_remaining -= 1;
                if fns_remaining == 0 {
                    fn_ready_tx.take();
                }

                // Mark `fn_id` as processed.
                fn_ids_processed.push(fn_id);

                graph_structure
                    .children(fn_id)
                    .iter(graph_structure)
                    .for_each(|(_edge_id, child_fn_id)| {
                        predecessor_counts[child_fn_id.index()] -= 1;
                        if predecessor_counts[child_fn_id.index()] == 0 {
                            if let Some(fn_ready_tx) = fn_ready_tx.as_ref() {
                                fn_ready_tx.try_send(child_fn_id).unwrap_or_else(
                                    #[cfg_attr(coverage_nightly, coverage(off))]
                                    |e| {
                                        panic!(
                                            "Failed to queue function `{}`. Cause: {}",
                                            fn_id.index(),
                                            e
                                        )
                                    },
                                );
                            }
                        }
                    });

                QueuerStreamState {
                    fns_remaining,
                    predecessor_counts,
                    fn_ready_tx,
                    fn_ids_processed,
                }
            },
        )
        .await;

    let QueuerStreamState {
        fn_ids_processed, ..
    } = queuer_stream_state;

    StreamOutcome::finished_with((), fn_ids_processed)
}

/// Polls the queuer stream until completed or interrupted.
#[cfg(all(feature = "async", feature = "interruptible"))]
async fn queuer_stream_fold_interruptible<'stream, StrmOutcome, S, IS>(
    stream: InterruptibleStream<'stream, S, IS>,
    queuer_stream_state_interruptible: QueuerStreamStateInterruptible,
    graph_structure: &Dag<(), Edge, FnIdInner>,
    fn_id_from_outcome: fn(StrmOutcome) -> (Option<NodeIndex<FnIdInner>>, bool),
) -> StreamOutcome<()>
where
    S: Stream<Item = NodeIndex<FnIdInner>>,
    InterruptibleStream<'stream, S, IS>: Stream<Item = StrmOutcome>,
    IS: InterruptStrategyT,
{
    let queuer_stream_state_interruptible = stream
        .fold(
            queuer_stream_state_interruptible,
            move |queuer_stream_state_interruptible, stream_outcome| async move {
                let QueuerStreamStateInterruptible {
                    mut fns_remaining,
                    mut predecessor_counts,
                    mut fn_ready_tx,
                    mut fn_graph_stream_progress,
                } = queuer_stream_state_interruptible;

                let StreamProgress {
                    value: (),
                    state,
                    ref mut fn_ids_processed,
                    ref mut fn_ids_not_processed,
                } = &mut fn_graph_stream_progress;

                // Close `fn_ready_rx` when all functions have been executed,
                fns_remaining -= 1;
                if fns_remaining == 0 {
                    *state = StreamProgressState::Finished;
                    fn_ready_tx.take();
                } else {
                    *state = StreamProgressState::InProgress;
                }

                let (fn_id, interrupted) = fn_id_from_outcome(stream_outcome);

                if let Some(fn_id) = fn_id {
                    // Mark `fn_id` as processed.
                    fn_ids_processed.push(fn_id);
                    if let Some(fn_id_pos) = fn_ids_not_processed
                        .iter()
                        .position(|fn_id_iter| fn_id == *fn_id_iter)
                    {
                        fn_ids_not_processed.remove(fn_id_pos);
                    }

                    if !interrupted {
                        graph_structure
                            .children(fn_id)
                            .iter(graph_structure)
                            .for_each(|(_edge_id, child_fn_id)| {
                                predecessor_counts[child_fn_id.index()] -= 1;
                                if predecessor_counts[child_fn_id.index()] == 0 {
                                    if let Some(fn_ready_tx) = fn_ready_tx.as_ref() {
                                        fn_ready_tx.try_send(child_fn_id).unwrap_or_else(
                                            #[cfg_attr(coverage_nightly, coverage(off))]
                                            |e| {
                                                panic!(
                                                    "Failed to queue function `{}`. Cause: {}",
                                                    fn_id.index(),
                                                    e
                                                )
                                            },
                                        );
                                    }
                                }
                            });
                    }
                } else {
                    todo!("");
                }

                QueuerStreamStateInterruptible {
                    fns_remaining,
                    predecessor_counts,
                    fn_ready_tx,
                    fn_graph_stream_progress,
                }
            },
        )
        .await;

    let QueuerStreamStateInterruptible {
        fn_graph_stream_progress,
        ..
    } = queuer_stream_state_interruptible;

    StreamOutcome::from(fn_graph_stream_progress)
}

struct StreamSetupInit<'f> {
    graph_structure: &'f Dag<(), Edge, FnIdInner>,
    predecessor_counts: Vec<usize>,
    fn_ready_tx: Sender<NodeIndex<FnIdInner>>,
    fn_ready_rx: Receiver<NodeIndex<FnIdInner>>,
    fn_done_tx: Sender<NodeIndex<FnIdInner>>,
    fn_done_rx: Receiver<NodeIndex<FnIdInner>>,
}

struct StreamSetupInitConcurrent<'f, QueuerFut> {
    graph_structure: &'f Dag<(), Edge, FnIdInner>,
    fn_ready_rx: Receiver<NodeIndex<FnIdInner>>,
    queuer: QueuerFut,
    fn_done_tx: RwLock<Option<Sender<NodeIndex<FnIdInner>>>>,
    fns_remaining: RwLock<usize>,
}

#[cfg(feature = "async")]
struct FoldStreamState<'f, F, Seed, FnFold> {
    /// Graph of functions.
    graph: &'f mut Dag<F, Edge, FnIdInner>,
    /// Number of functions in the graph remaining to execute.
    fns_remaining: usize,
    /// Channel sender for function IDs that have been run.
    fn_done_tx: Option<Sender<NodeIndex<FnIdInner>>>,
    /// Cumulative value after each fold.
    seed: Seed,
    /// Function to run each iteration.
    fn_fold: FnFold,
}

#[cfg(feature = "async")]
struct QueuerStreamState {
    /// Number of functions in the graph remaining to execute.
    fns_remaining: usize,
    /// Number of predecessors each function in the graph is waiting on.
    predecessor_counts: Vec<usize>,
    /// Channel sender for function IDs that are ready to run.
    fn_ready_tx: Option<Sender<NodeIndex<FnIdInner>>>,
    /// IDs of the items that are processed.
    fn_ids_processed: Vec<FnId>,
}

#[cfg(feature = "async")]
impl QueuerStreamState {
    fn new(
        fns_remaining: usize,
        predecessor_counts: Vec<usize>,
        fn_ready_tx: Option<Sender<NodeIndex<FnIdInner>>>,
    ) -> Self {
        Self {
            fns_remaining,
            predecessor_counts,
            fn_ready_tx,
            fn_ids_processed: Vec::with_capacity(fns_remaining),
        }
    }
}

#[cfg(all(feature = "async", feature = "interruptible"))]
struct QueuerStreamStateInterruptible {
    /// Number of functions in the graph remaining to execute.
    fns_remaining: usize,
    /// Number of predecessors each function in the graph is waiting on.
    predecessor_counts: Vec<usize>,
    /// Channel sender for function IDs that are ready to run.
    fn_ready_tx: Option<Sender<NodeIndex<FnIdInner>>>,
    /// State during processing a `FnGraph` stream.
    fn_graph_stream_progress: StreamProgress<()>,
}

impl<F> Default for FnGraph<F> {
    fn default() -> Self {
        Self {
            graph: Dag::new(),
            graph_structure: Dag::new(),
            graph_structure_rev: Dag::new(),
            ranks: Vec::new(),
            #[cfg(feature = "async")]
            edge_counts: EdgeCounts::default(),
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

impl<F> PartialEq for FnGraph<F>
where
    F: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        if self.graph.node_count() == other.graph.node_count()
            && self.graph.edge_count() == other.graph.edge_count()
        {
            let (ControlFlow::Continue(edge_eq) | ControlFlow::Break(edge_eq)) = self
                .graph
                .raw_edges()
                .iter()
                .zip(other.graph.raw_edges().iter())
                .try_fold(true, |_, (edge_self, edge_other)| {
                    if edge_self.source() == edge_other.source()
                        && edge_self.target() == edge_other.target()
                        && edge_self.weight == edge_other.weight
                    {
                        ControlFlow::Continue(true)
                    } else {
                        ControlFlow::Break(false)
                    }
                });

            if !edge_eq {
                return false;
            }

            let (ControlFlow::Continue(f_eq) | ControlFlow::Break(f_eq)) = self
                .iter_insertion()
                .zip(other.iter_insertion())
                .try_fold(true, |_, (f_self, f_other)| {
                    if f_self == f_other {
                        ControlFlow::Continue(true)
                    } else {
                        ControlFlow::Break(false)
                    }
                });

            f_eq
        } else {
            false
        }
    }
}

impl<F> Eq for FnGraph<F> where F: Eq {}

#[cfg(feature = "fn_meta")]
#[cfg(test)]
mod tests {
    use daggy::WouldCycle;
    use resman::{FnRes, IntoFnRes, Resources};

    use super::FnGraph;
    use crate::{Edge, FnGraphBuilder, FnId};

    #[test]
    fn clone() {
        let mut fn_graph = FnGraph::<u32>::new();
        fn_graph.add_node(0);

        let fn_graph_clone = fn_graph.clone();

        assert_eq!(fn_graph, fn_graph_clone);
    }

    #[test]
    fn debug() {
        let mut fn_graph = FnGraph::<u32>::new();
        fn_graph.add_node(0);

        let debug_str = format!("{fn_graph:?}");
        assert!(debug_str.starts_with("FnGraph {"));
    }

    #[test]
    fn new_returns_empty_graph() {
        let fn_graph = FnGraph::<Box<dyn FnRes<Ret = ()>>>::new();

        assert_eq!(0, fn_graph.node_count());
        assert!(fn_graph.ranks().is_empty());
    }

    #[test]
    fn partial_eq_returns_true_for_empty_graphs() {
        let fn_graph_a = FnGraph::<u32>::new();
        let fn_graph_b = FnGraph::<u32>::new();

        assert_eq!(fn_graph_a, fn_graph_b);
    }

    #[test]
    fn partial_eq_returns_false_for_different_value_graphs() {
        let mut fn_graph_a = FnGraph::<u32>::new();
        fn_graph_a.add_node(0);

        let mut fn_graph_b = FnGraph::<u32>::new();
        fn_graph_b.add_node(1);

        assert_ne!(fn_graph_a, fn_graph_b);
    }

    #[test]
    fn partial_eq_returns_true_for_same_value_same_edges_graphs() -> Result<(), WouldCycle<Edge>> {
        let mut fn_graph_a = FnGraph::<u32>::new();
        let a_0 = fn_graph_a.add_node(0);
        let a_1 = fn_graph_a.add_node(1);
        fn_graph_a.add_edge(a_0, a_1, Edge::Logic)?;

        let mut fn_graph_b = FnGraph::<u32>::new();
        let b_0 = fn_graph_b.add_node(0);
        let b_1 = fn_graph_b.add_node(1);
        fn_graph_b.add_edge(b_0, b_1, Edge::Logic)?;

        assert_eq!(fn_graph_a, fn_graph_b);
        Ok(())
    }

    #[test]
    fn partial_eq_returns_false_for_same_value_different_edge_direction_graphs()
    -> Result<(), WouldCycle<Edge>> {
        let mut fn_graph_a = FnGraph::<u32>::new();
        let a_0 = fn_graph_a.add_node(0);
        let a_1 = fn_graph_a.add_node(1);
        fn_graph_a.add_edge(a_0, a_1, Edge::Logic)?;

        let mut fn_graph_b = FnGraph::<u32>::new();
        let b_0 = fn_graph_b.add_node(0);
        let b_1 = fn_graph_b.add_node(1);
        fn_graph_b.add_edge(b_1, b_0, Edge::Logic)?;

        assert_ne!(fn_graph_a, fn_graph_b);
        Ok(())
    }

    #[test]
    fn partial_eq_returns_false_for_same_value_different_edge_type_graphs()
    -> Result<(), WouldCycle<Edge>> {
        let mut fn_graph_a = FnGraph::<u32>::new();
        let a_0 = fn_graph_a.add_node(0);
        let a_1 = fn_graph_a.add_node(1);
        fn_graph_a.add_edge(a_0, a_1, Edge::Logic)?;

        let mut fn_graph_b = FnGraph::<u32>::new();
        let b_0 = fn_graph_b.add_node(0);
        let b_1 = fn_graph_b.add_node(1);
        fn_graph_b.add_edge(b_0, b_1, Edge::Data)?;

        assert_ne!(fn_graph_a, fn_graph_b);
        Ok(())
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
    fn iter_rev_returns_fns_in_dep_rev_order() -> Result<(), WouldCycle<Edge>> {
        let fn_graph = complex_graph()?;

        let mut resources = Resources::new();
        resources.insert(0u8);
        resources.insert(0u16);
        let fn_iter_order = fn_graph
            .iter_rev()
            .map(|f| f.call(&resources))
            .collect::<Vec<_>>();

        assert_eq!(["e", "d", "c", "b", "a", "f"], fn_iter_order.as_slice());
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
        fn_graph.try_for_each(|f| {
            fn_iter_order.push(f.call(&resources));
            Ok(())
        })?;

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

        let call_fn = |f: &dyn FnRes<Ret = u32>| f.call(&resources);
        let mut fn_iter = fn_graph.iter_insertion();
        assert_eq!(Some(1), fn_iter.next().map(|f| call_fn(f.as_ref())));
        assert_eq!(Some(2), fn_iter.next().map(|f| call_fn(f.as_ref())));
        assert_eq!(None, fn_iter.next().map(|f| call_fn(f.as_ref())));
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
    fn iter_with_indices_returns_fns_with_indices() {
        let resources = Resources::new();
        let fn_graph = {
            let mut fn_graph_builder = FnGraphBuilder::new();
            fn_graph_builder.add_fn((|| 1u32).into_fn_res());
            fn_graph_builder.add_fn((|| 2u32).into_fn_res());
            fn_graph_builder.build()
        };

        let call_fn = |(fn_id, f): (FnId, &dyn FnRes<Ret = u32>)| (fn_id, f.call(&resources));
        let mut fn_iter = fn_graph.iter_insertion_with_indices();

        assert_eq!(
            Some((FnId::new(0), 1)),
            fn_iter.next().map(|(id, f)| call_fn((id, f.as_ref())))
        );
        assert_eq!(
            Some((FnId::new(1), 2)),
            fn_iter.next().map(|(id, f)| call_fn((id, f.as_ref())))
        );
        assert_eq!(
            None,
            fn_iter.next().map(|(id, f)| call_fn((id, f.as_ref())))
        );
    }

    #[cfg(feature = "async")]
    mod async_tests {
        use std::{fmt, ops::ControlFlow};

        use daggy::WouldCycle;
        use futures::{future::BoxFuture, stream, Future, FutureExt, StreamExt};
        use resman::{FnRes, FnResMut, IntoFnRes, IntoFnResMut, Resources};
        use tokio::{
            sync::mpsc::{self, error::TryRecvError, Receiver},
            time::{self, Duration, Instant},
        };

        use crate::{Edge, FnGraph, FnGraphBuilder, StreamOpts, StreamOutcome, StreamOutcomeState};

        macro_rules! sleep_duration {
            () => {
                Duration::from_millis(50)
            };
        }

        #[tokio::test]
        async fn stream_returns_when_graph_is_empty() {
            let fn_graph = FnGraph::<Box<dyn FnRes<Ret = ()>>>::new();

            fn_graph
                .stream()
                .for_each(
                    #[cfg_attr(coverage_nightly, coverage(off))]
                    |_f| async {},
                )
                .await;
        }

        #[tokio::test]
        async fn stream_returns_fns_in_dep_order_concurrently()
        -> Result<(), Box<dyn std::error::Error>> {
            let (fn_graph, mut seq_rx) = complex_graph_unit()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;
            test_timeout(
                Duration::from_millis(200),
                Duration::from_millis(255),
                fn_graph.stream().for_each_concurrent(None, |f| async move {
                    let _ = f.call(resources).await;
                }),
            )
            .await;

            let fn_iter_order = stream::poll_fn(|context| seq_rx.poll_recv(context))
                .take(fn_graph.node_count())
                .collect::<Vec<&'static str>>()
                .await;

            assert_eq!(["f", "a", "c", "b", "d", "e"], fn_iter_order.as_slice());
            Ok(())
        }

        #[tokio::test]
        async fn stream_rev_returns_when_graph_is_empty() {
            let fn_graph = FnGraph::<Box<dyn FnRes<Ret = ()>>>::new();

            fn_graph
                .stream_rev()
                .for_each(
                    #[cfg_attr(coverage_nightly, coverage(off))]
                    |_f| async {},
                )
                .await;
        }

        #[tokio::test]
        async fn stream_rev_returns_fns_in_dep_rev_order_concurrently()
        -> Result<(), Box<dyn std::error::Error>> {
            let (fn_graph, mut seq_rx) = complex_graph_unit()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;
            test_timeout(
                Duration::from_millis(200),
                Duration::from_millis(255),
                fn_graph
                    .stream_rev()
                    .for_each_concurrent(None, |f| async move {
                        let _ = f.call(resources).await;
                    }),
            )
            .await;

            let fn_iter_order = stream::poll_fn(|context| seq_rx.poll_recv(context))
                .take(fn_graph.node_count())
                .collect::<Vec<&'static str>>()
                .await;

            assert_eq!(["e", "d", "b", "c", "f", "a"], fn_iter_order.as_slice());
            Ok(())
        }

        #[tokio::test]
        async fn fold_async_returns_when_graph_is_empty() {
            let mut fn_graph = FnGraph::<Box<dyn FnRes<Ret = ()>>>::new();

            fn_graph
                .fold_async(
                    (),
                    #[cfg_attr(coverage_nightly, coverage(off))]
                    |(), _f| Box::pin(async {}),
                )
                .await;
        }

        #[tokio::test]
        async fn fold_async_runs_fns_in_dep_order_mut() -> Result<(), Box<dyn std::error::Error>> {
            let (mut fn_graph, mut seq_rx) = complex_graph_unit_mut()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            test_timeout(
                Duration::from_millis(200),
                Duration::from_millis(385), // On Windows the duration can be much higher
                fn_graph.fold_async(resources, |resources, f| {
                    Box::pin(async move {
                        f.call_mut(&resources).await;
                        resources
                    })
                }),
            )
            .await;

            let fn_iter_order = stream::poll_fn(|context| seq_rx.poll_recv(context))
                .collect::<Vec<&'static str>>()
                .await;

            assert_eq!(["f", "a", "c", "b", "d", "e"], fn_iter_order.as_slice());
            Ok(())
        }

        #[tokio::test]
        async fn fold_async_with_returns_when_graph_is_empty() {
            let mut fn_graph = FnGraph::<Box<dyn FnRes<Ret = ()>>>::new();

            fn_graph
                .fold_async_with(
                    (),
                    StreamOpts::new().rev(),
                    #[cfg_attr(coverage_nightly, coverage(off))]
                    |(), _f| Box::pin(async {}),
                )
                .await;
        }

        #[tokio::test]
        async fn fold_async_with_runs_fns_in_dep_rev_order_mut()
        -> Result<(), Box<dyn std::error::Error>> {
            let (mut fn_graph, mut seq_rx) = complex_graph_unit_mut()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            test_timeout(
                Duration::from_millis(200),
                Duration::from_millis(385), // On Windows the duration can be much higher
                fn_graph.fold_async_with(resources, StreamOpts::new().rev(), |resources, f| {
                    Box::pin(async move {
                        f.call_mut(&resources).await;
                        resources
                    })
                }),
            )
            .await;

            let fn_iter_order = stream::poll_fn(|context| seq_rx.poll_recv(context))
                .collect::<Vec<&'static str>>()
                .await;

            assert_eq!(["e", "d", "b", "c", "f", "a"], fn_iter_order.as_slice());
            Ok(())
        }

        #[tokio::test]
        async fn for_each_concurrent_returns_when_graph_is_empty() {
            let fn_graph = FnGraph::<Box<dyn FnRes<Ret = ()>>>::new();

            fn_graph
                .for_each_concurrent(
                    None,
                    #[cfg_attr(coverage_nightly, coverage(off))]
                    |_f| async {},
                )
                .await;
        }

        #[tokio::test]
        async fn for_each_concurrent_runs_fns_concurrently()
        -> Result<(), Box<dyn std::error::Error>> {
            let (fn_graph, mut seq_rx) = complex_graph_unit()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;
            test_timeout(
                Duration::from_millis(200),
                Duration::from_millis(255),
                fn_graph.for_each_concurrent(None, |f| {
                    let fut = f.call(resources);
                    async move {
                        let _ = fut.await;
                    }
                }),
            )
            .await;

            seq_rx.close();
            let fn_iter_order = stream::poll_fn(|context| seq_rx.poll_recv(context))
                .collect::<Vec<&'static str>>()
                .await;

            assert_eq!(["f", "a", "c", "b", "d", "e"], fn_iter_order.as_slice());
            Ok(())
        }

        #[tokio::test]
        async fn for_each_concurrent_with_returns_when_graph_is_empty() {
            let fn_graph = FnGraph::<Box<dyn FnRes<Ret = ()>>>::new();

            fn_graph
                .for_each_concurrent_with(
                    None,
                    StreamOpts::new().rev(),
                    #[cfg_attr(coverage_nightly, coverage(off))]
                    |_f| async {},
                )
                .await;
        }

        #[tokio::test]
        async fn for_each_concurrent_with_runs_fns_concurrently()
        -> Result<(), Box<dyn std::error::Error>> {
            let (fn_graph, mut seq_rx) = complex_graph_unit()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;
            test_timeout(
                Duration::from_millis(200),
                Duration::from_millis(255),
                fn_graph.for_each_concurrent_with(None, StreamOpts::new().rev(), |f| {
                    let fut = f.call(resources);
                    async move {
                        let _ = fut.await;
                    }
                }),
            )
            .await;

            seq_rx.close();
            let fn_iter_order = stream::poll_fn(|context| seq_rx.poll_recv(context))
                .collect::<Vec<&'static str>>()
                .await;

            assert_eq!(["e", "d", "b", "c", "f", "a"], fn_iter_order.as_slice());
            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_returns_when_graph_is_empty() -> Result<(), ()> {
            let fn_graph = FnGraph::<Box<dyn FnRes<Ret = ()>>>::new();

            let fn_graph_stream_outcome = fn_graph
                .try_for_each_concurrent(
                    None,
                    #[cfg_attr(coverage_nightly, coverage(off))]
                    |_f| async { Ok::<_, ()>(()) },
                )
                .await
                .map_err(|_| ())?;

            assert_eq!(
                StreamOutcome {
                    value: (),
                    state: StreamOutcomeState::Finished,
                    fn_ids_processed: Vec::new(),
                    fn_ids_not_processed: Vec::new(),
                },
                fn_graph_stream_outcome
            );

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_runs_fns_concurrently()
        -> Result<(), Box<dyn std::error::Error>> {
            let (fn_graph, mut seq_rx) = complex_graph_unit()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            test_timeout(
                Duration::from_millis(200),
                Duration::from_millis(255),
                fn_graph.try_for_each_concurrent(None, |f| {
                    let fut = f.call(resources);
                    async move {
                        let _ = fut.await;
                        Result::<_, TestError>::Ok(())
                    }
                }),
            )
            .await
            .unwrap();

            seq_rx.close();
            let fn_iter_order = stream::poll_fn(|context| seq_rx.poll_recv(context))
                .collect::<Vec<&'static str>>()
                .await;

            assert_eq!(["f", "a", "c", "b", "d", "e"], fn_iter_order.as_slice());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_gracefully_ends_when_one_function_returns_failure()
        -> Result<(), Box<dyn std::error::Error>> {
            let (fn_graph, mut seq_rx) = complex_graph_unit()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let result = test_timeout(
                Duration::from_millis(50),
                Duration::from_millis(70),
                fn_graph.try_for_each_concurrent(None, |f| {
                    let fut = f.call(resources);
                    async move {
                        match fut.await {
                            "a" => Err(TestError("a")),
                            _ => Ok(()),
                        }
                    }
                }),
            )
            .await;

            assert_eq!([TestError("a")], result.unwrap_err().1.as_slice());
            assert_eq!("f", seq_rx.try_recv().unwrap());
            assert_eq!("a", seq_rx.try_recv().unwrap()); // "a" is sent before we err
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_gracefully_ends_when_one_function_returns_failure_variation()
        -> Result<(), Box<dyn std::error::Error>> {
            let (fn_graph, mut seq_rx) = complex_graph_unit()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let result = test_timeout(
                Duration::from_millis(100),
                Duration::from_millis(130),
                fn_graph.try_for_each_concurrent(None, |f| {
                    let fut = f.call(resources);
                    async move {
                        match fut.await {
                            "c" => Err(TestError("c")),
                            _ => Ok(()),
                        }
                    }
                }),
            )
            .await;

            assert_eq!([TestError("c")], result.unwrap_err().1.as_slice());
            assert_eq!("f", seq_rx.try_recv().unwrap());
            assert_eq!("a", seq_rx.try_recv().unwrap());
            assert_eq!("c", seq_rx.try_recv().unwrap());
            assert_eq!("b", seq_rx.try_recv().unwrap());
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_gracefully_ends_when_multiple_functions_return_failure()
        -> Result<(), Box<dyn std::error::Error>> {
            let (fn_graph, mut seq_rx) = complex_graph_unit()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let result = test_timeout(
                Duration::from_millis(100),
                Duration::from_millis(130),
                fn_graph.try_for_each_concurrent(None, |f| {
                    let fut = f.call(resources);
                    async move {
                        match fut.await {
                            "b" => Err(TestError("b")),
                            "c" => Err(TestError("c")),
                            _ => Ok(()),
                        }
                    }
                }),
            )
            .await;

            // Both "c" and "b" being present proves we have waited for in-progress tasks to
            // complete.
            assert_eq!(
                [TestError("c"), TestError("b")],
                result.unwrap_err().1.as_slice()
            );
            assert_eq!("f", seq_rx.try_recv().unwrap());
            assert_eq!("a", seq_rx.try_recv().unwrap());
            assert_eq!("c", seq_rx.try_recv().unwrap());
            assert_eq!("b", seq_rx.try_recv().unwrap());
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_with_returns_when_graph_is_empty() -> Result<(), ()> {
            let fn_graph = FnGraph::<Box<dyn FnRes<Ret = ()>>>::new();

            let fn_graph_stream_outcome = fn_graph
                .try_for_each_concurrent_with(
                    None,
                    StreamOpts::new().rev(),
                    #[cfg_attr(coverage_nightly, coverage(off))]
                    |_f| async { Ok::<_, ()>(()) },
                )
                .await
                .map_err(|_| ())?;

            assert_eq!(
                StreamOutcome {
                    value: (),
                    state: StreamOutcomeState::Finished,
                    fn_ids_processed: Vec::new(),
                    fn_ids_not_processed: Vec::new(),
                },
                fn_graph_stream_outcome
            );

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_with_runs_fns_concurrently()
        -> Result<(), Box<dyn std::error::Error>> {
            let (fn_graph, mut seq_rx) = complex_graph_unit()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            test_timeout(
                Duration::from_millis(200),
                Duration::from_millis(255),
                fn_graph.try_for_each_concurrent_with(None, StreamOpts::new().rev(), |f| {
                    let fut = f.call(resources);
                    async move {
                        let _ = fut.await;
                        Result::<_, TestError>::Ok(())
                    }
                }),
            )
            .await
            .unwrap();

            seq_rx.close();
            let fn_iter_order = stream::poll_fn(|context| seq_rx.poll_recv(context))
                .collect::<Vec<&'static str>>()
                .await;

            assert_eq!(["e", "d", "b", "c", "f", "a"], fn_iter_order.as_slice());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_with_gracefully_ends_when_one_function_returns_failure()
        -> Result<(), Box<dyn std::error::Error>> {
            let (fn_graph, mut seq_rx) = complex_graph_unit()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let result = test_timeout(
                Duration::from_millis(50),
                Duration::from_millis(70),
                fn_graph.try_for_each_concurrent_with(None, StreamOpts::new().rev(), |f| {
                    let fut = f.call(resources);
                    async move {
                        match fut.await {
                            "e" => Err(TestError("e")),
                            _ => Ok(()),
                        }
                    }
                }),
            )
            .await;

            assert_eq!([TestError("e")], result.unwrap_err().1.as_slice());
            assert_eq!("e", seq_rx.try_recv().unwrap()); // "a" is sent before we err
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_with_gracefully_ends_when_one_function_returns_failure_variation()
        -> Result<(), Box<dyn std::error::Error>> {
            let (fn_graph, mut seq_rx) = complex_graph_unit()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let result = test_timeout(
                Duration::from_millis(150),
                Duration::from_millis(197),
                fn_graph.try_for_each_concurrent_with(None, StreamOpts::new().rev(), |f| {
                    let fut = f.call(resources);
                    async move {
                        match fut.await {
                            "b" => Err(TestError("b")),
                            _ => Ok(()),
                        }
                    }
                }),
            )
            .await;

            assert_eq!([TestError("b")], result.unwrap_err().1.as_slice());
            assert_eq!("e", seq_rx.try_recv().unwrap());
            assert_eq!("d", seq_rx.try_recv().unwrap());
            assert_eq!("b", seq_rx.try_recv().unwrap());
            assert_eq!("c", seq_rx.try_recv().unwrap());
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_with_gracefully_ends_when_multiple_functions_return_failure()
        -> Result<(), Box<dyn std::error::Error>> {
            let (fn_graph, mut seq_rx) = complex_graph_unit()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let result = test_timeout(
                Duration::from_millis(150),
                Duration::from_millis(197),
                fn_graph.try_for_each_concurrent_with(None, StreamOpts::new().rev(), |f| {
                    let fut = f.call(resources);
                    async move {
                        match fut.await {
                            "b" => Err(TestError("b")),
                            "c" => Err(TestError("c")),
                            _ => Ok(()),
                        }
                    }
                }),
            )
            .await;

            assert_eq!(
                [TestError("b"), TestError("c")],
                result.unwrap_err().1.as_slice()
            );
            assert_eq!("e", seq_rx.try_recv().unwrap());
            assert_eq!("d", seq_rx.try_recv().unwrap());
            assert_eq!("b", seq_rx.try_recv().unwrap());
            assert_eq!("c", seq_rx.try_recv().unwrap());
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        #[tokio::test]
        async fn for_each_concurrent_mut_runs_fns_concurrently()
        -> Result<(), Box<dyn std::error::Error>> {
            let (mut fn_graph, mut seq_rx) = complex_graph_unit_mut()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;
            test_timeout(
                Duration::from_millis(200),
                Duration::from_millis(255),
                fn_graph.for_each_concurrent_mut(None, |f| {
                    let fut = f.call_mut(resources);
                    async move {
                        let _ = fut.await;
                    }
                }),
            )
            .await;

            let fn_iter_order = stream::poll_fn(|context| seq_rx.poll_recv(context))
                .collect::<Vec<&'static str>>()
                .await;

            assert_eq!(["f", "a", "c", "b", "d", "e"], fn_iter_order.as_slice());
            Ok(())
        }

        #[tokio::test]
        async fn for_each_concurrent_mut_with_runs_fns_concurrently()
        -> Result<(), Box<dyn std::error::Error>> {
            let (mut fn_graph, mut seq_rx) = complex_graph_unit_mut()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;
            test_timeout(
                Duration::from_millis(200),
                Duration::from_millis(255),
                fn_graph.for_each_concurrent_mut_with(None, StreamOpts::new().rev(), |f| {
                    let fut = f.call_mut(resources);
                    async move {
                        let _ = fut.await;
                    }
                }),
            )
            .await;

            let fn_iter_order = stream::poll_fn(|context| seq_rx.poll_recv(context))
                .collect::<Vec<&'static str>>()
                .await;

            assert_eq!(["e", "d", "b", "c", "f", "a"], fn_iter_order.as_slice());
            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_mut_runs_fns_concurrently_mut()
        -> Result<(), Box<dyn std::error::Error>> {
            let (mut fn_graph, mut seq_rx) = complex_graph_unit_mut()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            test_timeout(
                Duration::from_millis(200),
                Duration::from_millis(255),
                fn_graph.try_for_each_concurrent_mut(None, |f| {
                    let fut = f.call_mut(resources);
                    async move {
                        let _ = fut.await;
                        Result::<_, TestError>::Ok(())
                    }
                }),
            )
            .await
            .unwrap();

            let fn_iter_order = stream::poll_fn(|context| seq_rx.poll_recv(context))
                .collect::<Vec<&'static str>>()
                .await;

            assert_eq!(["f", "a", "c", "b", "d", "e"], fn_iter_order.as_slice());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_mut_gracefully_ends_when_one_function_returns_failure()
        -> Result<(), Box<dyn std::error::Error>> {
            let (mut fn_graph, mut seq_rx) = complex_graph_unit_mut()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let result = test_timeout(
                Duration::from_millis(50),
                Duration::from_millis(70),
                fn_graph.try_for_each_concurrent_mut(None, |f| {
                    let fut = f.call_mut(resources);
                    async move {
                        match fut.await {
                            "a" => Err(TestError("a")),
                            _ => Ok(()),
                        }
                    }
                }),
            )
            .await;

            assert_eq!([TestError("a")], result.unwrap_err().1.as_slice());
            assert_eq!("f", seq_rx.try_recv().unwrap());
            assert_eq!("a", seq_rx.try_recv().unwrap()); // "a" is sent before we err
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_mut_gracefully_ends_when_one_function_returns_failure_variation()
        -> Result<(), Box<dyn std::error::Error>> {
            let (mut fn_graph, mut seq_rx) = complex_graph_unit_mut()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let result = test_timeout(
                Duration::from_millis(100),
                Duration::from_millis(130),
                fn_graph.try_for_each_concurrent_mut(None, |f| {
                    let fut = f.call_mut(resources);
                    async move {
                        match fut.await {
                            "c" => Err(TestError("c")),
                            _ => Ok(()),
                        }
                    }
                }),
            )
            .await;

            assert_eq!([TestError("c")], result.unwrap_err().1.as_slice());
            assert_eq!("f", seq_rx.try_recv().unwrap());
            assert_eq!("a", seq_rx.try_recv().unwrap());
            assert_eq!("c", seq_rx.try_recv().unwrap());
            assert_eq!("b", seq_rx.try_recv().unwrap());
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_mut_gracefully_ends_when_multiple_functions_return_failure()
        -> Result<(), Box<dyn std::error::Error>> {
            let (mut fn_graph, mut seq_rx) = complex_graph_unit_mut()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let result = test_timeout(
                Duration::from_millis(100),
                Duration::from_millis(130),
                fn_graph.try_for_each_concurrent_mut(None, |f| {
                    let fut = f.call_mut(resources);
                    async move {
                        match fut.await {
                            "b" => Err(TestError("b")),
                            "c" => Err(TestError("c")),
                            _ => Ok(()),
                        }
                    }
                }),
            )
            .await;

            // Both "c" and "b" being present proves we have waited for in-progress tasks to
            // complete.
            assert_eq!(
                [TestError("c"), TestError("b")],
                result.unwrap_err().1.as_slice()
            );
            assert_eq!("f", seq_rx.try_recv().unwrap());
            assert_eq!("a", seq_rx.try_recv().unwrap());
            assert_eq!("c", seq_rx.try_recv().unwrap());
            assert_eq!("b", seq_rx.try_recv().unwrap());
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_mut_with_runs_fns_concurrently_mut()
        -> Result<(), Box<dyn std::error::Error>> {
            let (mut fn_graph, mut seq_rx) = complex_graph_unit_mut()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            test_timeout(
                Duration::from_millis(200),
                Duration::from_millis(255),
                fn_graph.try_for_each_concurrent_mut_with(None, StreamOpts::new().rev(), |f| {
                    let fut = f.call_mut(resources);
                    async move {
                        let _ = fut.await;
                        Result::<_, TestError>::Ok(())
                    }
                }),
            )
            .await
            .unwrap();

            let fn_iter_order = stream::poll_fn(|context| seq_rx.poll_recv(context))
                .collect::<Vec<&'static str>>()
                .await;

            assert_eq!(["e", "d", "b", "c", "f", "a"], fn_iter_order.as_slice());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_mut_with_gracefully_ends_when_one_function_returns_failure()
        -> Result<(), Box<dyn std::error::Error>> {
            let (mut fn_graph, mut seq_rx) = complex_graph_unit_mut()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let result = test_timeout(
                Duration::from_millis(50),
                Duration::from_millis(70),
                fn_graph.try_for_each_concurrent_mut_with(None, StreamOpts::new().rev(), |f| {
                    let fut = f.call_mut(resources);
                    async move {
                        match fut.await {
                            "e" => Err(TestError("e")),
                            _ => Ok(()),
                        }
                    }
                }),
            )
            .await;

            assert_eq!([TestError("e")], result.unwrap_err().1.as_slice());
            assert_eq!("e", seq_rx.try_recv().unwrap()); // "a" is sent before we err
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_mut_with_gracefully_ends_when_one_function_returns_failure_variation()
        -> Result<(), Box<dyn std::error::Error>> {
            let (mut fn_graph, mut seq_rx) = complex_graph_unit_mut()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let result = test_timeout(
                Duration::from_millis(150),
                Duration::from_millis(197),
                fn_graph.try_for_each_concurrent_mut_with(None, StreamOpts::new().rev(), |f| {
                    let fut = f.call_mut(resources);
                    async move {
                        match fut.await {
                            "b" => Err(TestError("b")),
                            _ => Ok(()),
                        }
                    }
                }),
            )
            .await;

            assert_eq!([TestError("b")], result.unwrap_err().1.as_slice());
            assert_eq!("e", seq_rx.try_recv().unwrap());
            assert_eq!("d", seq_rx.try_recv().unwrap());
            assert_eq!("b", seq_rx.try_recv().unwrap());
            assert_eq!("c", seq_rx.try_recv().unwrap());
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_mut_with_gracefully_ends_when_multiple_functions_return_failure()
        -> Result<(), Box<dyn std::error::Error>> {
            let (mut fn_graph, mut seq_rx) = complex_graph_unit_mut()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let result = test_timeout(
                Duration::from_millis(150),
                Duration::from_millis(197),
                fn_graph.try_for_each_concurrent_mut_with(None, StreamOpts::new().rev(), |f| {
                    let fut = f.call_mut(resources);
                    async move {
                        match fut.await {
                            "b" => Err(TestError("b")),
                            "c" => Err(TestError("c")),
                            _ => Ok(()),
                        }
                    }
                }),
            )
            .await;

            assert_eq!(
                [TestError("b"), TestError("c")],
                result.unwrap_err().1.as_slice()
            );
            assert_eq!("e", seq_rx.try_recv().unwrap());
            assert_eq!("d", seq_rx.try_recv().unwrap());
            assert_eq!("b", seq_rx.try_recv().unwrap());
            assert_eq!("c", seq_rx.try_recv().unwrap());
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_control_returns_when_graph_is_empty() {
            let fn_graph = FnGraph::<Box<dyn FnRes<Ret = ()>>>::new();

            let (ControlFlow::Continue(_) | ControlFlow::Break(_)) = fn_graph
                .try_for_each_concurrent_control(
                    None,
                    #[cfg_attr(coverage_nightly, coverage(off))]
                    |_f| async { ControlFlow::<(), ()>::Continue(()) },
                )
                .await;
        }

        #[tokio::test]
        async fn try_for_each_concurrent_control_runs_fns_concurrently()
        -> Result<(), Box<dyn std::error::Error>> {
            let (fn_graph, mut seq_rx) = complex_graph_unit()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let (ControlFlow::Continue(_) | ControlFlow::Break(_)) = test_timeout(
                Duration::from_millis(200),
                Duration::from_millis(255),
                fn_graph.try_for_each_concurrent_control(None, |f| {
                    let fut = f.call(resources);
                    async move {
                        let _ = fut.await;
                        ControlFlow::<(), ()>::Continue(())
                    }
                }),
            )
            .await;

            seq_rx.close();
            let fn_iter_order = stream::poll_fn(|context| seq_rx.poll_recv(context))
                .collect::<Vec<&'static str>>()
                .await;

            assert_eq!(["f", "a", "c", "b", "d", "e"], fn_iter_order.as_slice());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_control_gracefully_ends_when_one_function_returns_failure()
        -> Result<(), Box<dyn std::error::Error>> {
            let (fn_graph, mut seq_rx) = complex_graph_unit()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let (ControlFlow::Continue(_) | ControlFlow::Break(_)) = test_timeout(
                Duration::from_millis(50),
                Duration::from_millis(70),
                fn_graph.try_for_each_concurrent_control(None, |f| {
                    let fut = f.call(resources);
                    async move {
                        match fut.await {
                            "a" => ControlFlow::Break(()),
                            _ => ControlFlow::Continue(()),
                        }
                    }
                }),
            )
            .await;

            assert_eq!("f", seq_rx.try_recv().unwrap());
            assert_eq!("a", seq_rx.try_recv().unwrap()); // "a" is sent before we err
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_control_gracefully_ends_when_one_function_returns_failure_variation()
        -> Result<(), Box<dyn std::error::Error>> {
            let (fn_graph, mut seq_rx) = complex_graph_unit()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let (ControlFlow::Continue(_) | ControlFlow::Break(_)) = test_timeout(
                Duration::from_millis(100),
                Duration::from_millis(130),
                fn_graph.try_for_each_concurrent_control(None, |f| {
                    let fut = f.call(resources);
                    async move {
                        match fut.await {
                            "c" => ControlFlow::Break(()),
                            _ => ControlFlow::Continue(()),
                        }
                    }
                }),
            )
            .await;

            assert_eq!("f", seq_rx.try_recv().unwrap());
            assert_eq!("a", seq_rx.try_recv().unwrap());
            assert_eq!("c", seq_rx.try_recv().unwrap());
            assert_eq!("b", seq_rx.try_recv().unwrap());
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_control_gracefully_ends_when_multiple_functions_return_failure()
        -> Result<(), Box<dyn std::error::Error>> {
            let (fn_graph, mut seq_rx) = complex_graph_unit()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let (ControlFlow::Continue(_) | ControlFlow::Break(_)) = test_timeout(
                Duration::from_millis(100),
                Duration::from_millis(130),
                fn_graph.try_for_each_concurrent_control(None, |f| {
                    let fut = f.call(resources);
                    async move {
                        match fut.await {
                            "b" => ControlFlow::Break(()),
                            "c" => ControlFlow::Break(()),
                            _ => ControlFlow::Continue(()),
                        }
                    }
                }),
            )
            .await;

            assert_eq!("f", seq_rx.try_recv().unwrap());
            assert_eq!("a", seq_rx.try_recv().unwrap());
            assert_eq!("c", seq_rx.try_recv().unwrap());
            assert_eq!("b", seq_rx.try_recv().unwrap());
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_control_with_returns_when_graph_is_empty() {
            let fn_graph = FnGraph::<Box<dyn FnRes<Ret = ()>>>::new();

            let (ControlFlow::Continue(_) | ControlFlow::Break(_)) = fn_graph
                .try_for_each_concurrent_control_with(
                    None,
                    StreamOpts::new().rev(),
                    #[cfg_attr(coverage_nightly, coverage(off))]
                    |_f| async { ControlFlow::<(), ()>::Continue(()) },
                )
                .await;
        }

        #[tokio::test]
        async fn try_for_each_concurrent_control_with_runs_fns_concurrently()
        -> Result<(), Box<dyn std::error::Error>> {
            let (fn_graph, mut seq_rx) = complex_graph_unit()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let (ControlFlow::Continue(_) | ControlFlow::Break(_)) = test_timeout(
                Duration::from_millis(200),
                Duration::from_millis(255),
                fn_graph.try_for_each_concurrent_control_with(None, StreamOpts::new().rev(), |f| {
                    let fut = f.call(resources);
                    async move {
                        let _ = fut.await;
                        ControlFlow::<(), ()>::Continue(())
                    }
                }),
            )
            .await;

            seq_rx.close();
            let fn_iter_order = stream::poll_fn(|context| seq_rx.poll_recv(context))
                .collect::<Vec<&'static str>>()
                .await;

            assert_eq!(["e", "d", "b", "c", "f", "a"], fn_iter_order.as_slice());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_control_with_gracefully_ends_when_one_function_returns_failure()
        -> Result<(), Box<dyn std::error::Error>> {
            let (fn_graph, mut seq_rx) = complex_graph_unit()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let (ControlFlow::Continue(_) | ControlFlow::Break(_)) = test_timeout(
                Duration::from_millis(50),
                Duration::from_millis(70),
                fn_graph.try_for_each_concurrent_control_with(None, StreamOpts::new().rev(), |f| {
                    let fut = f.call(resources);
                    async move {
                        match fut.await {
                            "e" => ControlFlow::Break(()),
                            _ => ControlFlow::Continue(()),
                        }
                    }
                }),
            )
            .await;

            assert_eq!("e", seq_rx.try_recv().unwrap()); // "a" is sent before we err
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_control_with_gracefully_ends_when_one_function_returns_failure_variation()
        -> Result<(), Box<dyn std::error::Error>> {
            let (fn_graph, mut seq_rx) = complex_graph_unit()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let (ControlFlow::Continue(_) | ControlFlow::Break(_)) = test_timeout(
                Duration::from_millis(150),
                Duration::from_millis(197),
                fn_graph.try_for_each_concurrent_control_with(None, StreamOpts::new().rev(), |f| {
                    let fut = f.call(resources);
                    async move {
                        match fut.await {
                            "b" => ControlFlow::Break(()),
                            _ => ControlFlow::Continue(()),
                        }
                    }
                }),
            )
            .await;

            assert_eq!("e", seq_rx.try_recv().unwrap());
            assert_eq!("d", seq_rx.try_recv().unwrap());
            assert_eq!("b", seq_rx.try_recv().unwrap());
            assert_eq!("c", seq_rx.try_recv().unwrap());
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_control_with_gracefully_ends_when_multiple_functions_return_failure()
        -> Result<(), Box<dyn std::error::Error>> {
            let (fn_graph, mut seq_rx) = complex_graph_unit()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let (ControlFlow::Continue(_) | ControlFlow::Break(_)) = test_timeout(
                Duration::from_millis(150),
                Duration::from_millis(197),
                fn_graph.try_for_each_concurrent_control_with(None, StreamOpts::new().rev(), |f| {
                    let fut = f.call(resources);
                    async move {
                        match fut.await {
                            "b" => ControlFlow::Break(()),
                            "c" => ControlFlow::Break(()),
                            _ => ControlFlow::Continue(()),
                        }
                    }
                }),
            )
            .await;

            assert_eq!("e", seq_rx.try_recv().unwrap());
            assert_eq!("d", seq_rx.try_recv().unwrap());
            assert_eq!("b", seq_rx.try_recv().unwrap());
            assert_eq!("c", seq_rx.try_recv().unwrap());
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_control_mut_runs_fns_concurrently_mut()
        -> Result<(), Box<dyn std::error::Error>> {
            let (mut fn_graph, mut seq_rx) = complex_graph_unit_mut()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let (ControlFlow::Continue(_) | ControlFlow::Break(_)) = test_timeout(
                Duration::from_millis(200),
                Duration::from_millis(255),
                fn_graph.try_for_each_concurrent_control_mut(None, |f| {
                    let fut = f.call_mut(resources);
                    async move {
                        let _ = fut.await;
                        ControlFlow::<(), ()>::Continue(())
                    }
                }),
            )
            .await;

            let fn_iter_order = stream::poll_fn(|context| seq_rx.poll_recv(context))
                .collect::<Vec<&'static str>>()
                .await;

            assert_eq!(["f", "a", "c", "b", "d", "e"], fn_iter_order.as_slice());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_control_mut_gracefully_ends_when_one_function_returns_failure()
        -> Result<(), Box<dyn std::error::Error>> {
            let (mut fn_graph, mut seq_rx) = complex_graph_unit_mut()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let (ControlFlow::Continue(_) | ControlFlow::Break(_)) = test_timeout(
                Duration::from_millis(50),
                Duration::from_millis(70),
                fn_graph.try_for_each_concurrent_control_mut(None, |f| {
                    let fut = f.call_mut(resources);
                    async move {
                        match fut.await {
                            "a" => ControlFlow::Break(()),
                            _ => ControlFlow::Continue(()),
                        }
                    }
                }),
            )
            .await;

            assert_eq!("f", seq_rx.try_recv().unwrap());
            assert_eq!("a", seq_rx.try_recv().unwrap()); // "a" is sent before we err
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_control_mut_gracefully_ends_when_one_function_returns_failure_variation()
        -> Result<(), Box<dyn std::error::Error>> {
            let (mut fn_graph, mut seq_rx) = complex_graph_unit_mut()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let (ControlFlow::Continue(_) | ControlFlow::Break(_)) = test_timeout(
                Duration::from_millis(100),
                Duration::from_millis(130),
                fn_graph.try_for_each_concurrent_control_mut(None, |f| {
                    let fut = f.call_mut(resources);
                    async move {
                        match fut.await {
                            "c" => ControlFlow::Break(()),
                            _ => ControlFlow::Continue(()),
                        }
                    }
                }),
            )
            .await;

            assert_eq!("f", seq_rx.try_recv().unwrap());
            assert_eq!("a", seq_rx.try_recv().unwrap());
            assert_eq!("c", seq_rx.try_recv().unwrap());
            assert_eq!("b", seq_rx.try_recv().unwrap());
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_control_mut_gracefully_ends_when_multiple_functions_return_failure()
        -> Result<(), Box<dyn std::error::Error>> {
            let (mut fn_graph, mut seq_rx) = complex_graph_unit_mut()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let (ControlFlow::Continue(_) | ControlFlow::Break(_)) = test_timeout(
                Duration::from_millis(100),
                Duration::from_millis(130),
                fn_graph.try_for_each_concurrent_control_mut(None, |f| {
                    let fut = f.call_mut(resources);
                    async move {
                        match fut.await {
                            "b" => ControlFlow::Break(()),
                            "c" => ControlFlow::Break(()),
                            _ => ControlFlow::Continue(()),
                        }
                    }
                }),
            )
            .await;

            assert_eq!("f", seq_rx.try_recv().unwrap());
            assert_eq!("a", seq_rx.try_recv().unwrap());
            assert_eq!("c", seq_rx.try_recv().unwrap());
            assert_eq!("b", seq_rx.try_recv().unwrap());
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_control_mut_with_runs_fns_concurrently_mut()
        -> Result<(), Box<dyn std::error::Error>> {
            let (mut fn_graph, mut seq_rx) = complex_graph_unit_mut()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let (ControlFlow::Continue(_) | ControlFlow::Break(_)) = test_timeout(
                Duration::from_millis(200),
                Duration::from_millis(255),
                fn_graph.try_for_each_concurrent_control_mut_with(
                    None,
                    StreamOpts::new().rev(),
                    |f| {
                        let fut = f.call_mut(resources);
                        async move {
                            let _ = fut.await;
                            ControlFlow::<(), ()>::Continue(())
                        }
                    },
                ),
            )
            .await;

            let fn_iter_order = stream::poll_fn(|context| seq_rx.poll_recv(context))
                .collect::<Vec<&'static str>>()
                .await;

            assert_eq!(["e", "d", "b", "c", "f", "a"], fn_iter_order.as_slice());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_control_mut_with_gracefully_ends_when_one_function_returns_failure()
        -> Result<(), Box<dyn std::error::Error>> {
            let (mut fn_graph, mut seq_rx) = complex_graph_unit_mut()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let (ControlFlow::Continue(_) | ControlFlow::Break(_)) = test_timeout(
                Duration::from_millis(50),
                Duration::from_millis(70),
                fn_graph.try_for_each_concurrent_control_mut_with(
                    None,
                    StreamOpts::new().rev(),
                    |f| {
                        let fut = f.call_mut(resources);
                        async move {
                            match fut.await {
                                "e" => ControlFlow::Break(()),
                                _ => ControlFlow::Continue(()),
                            }
                        }
                    },
                ),
            )
            .await;

            assert_eq!("e", seq_rx.try_recv().unwrap()); // "a" is sent before we err
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_control_mut_with_gracefully_ends_when_one_function_returns_failure_variation()
        -> Result<(), Box<dyn std::error::Error>> {
            let (mut fn_graph, mut seq_rx) = complex_graph_unit_mut()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let (ControlFlow::Continue(_) | ControlFlow::Break(_)) = test_timeout(
                Duration::from_millis(150),
                Duration::from_millis(197),
                fn_graph.try_for_each_concurrent_control_mut_with(
                    None,
                    StreamOpts::new().rev(),
                    |f| {
                        let fut = f.call_mut(resources);
                        async move {
                            match fut.await {
                                "b" => ControlFlow::Break(()),
                                _ => ControlFlow::Continue(()),
                            }
                        }
                    },
                ),
            )
            .await;

            assert_eq!("e", seq_rx.try_recv().unwrap());
            assert_eq!("d", seq_rx.try_recv().unwrap());
            assert_eq!("b", seq_rx.try_recv().unwrap());
            assert_eq!("c", seq_rx.try_recv().unwrap());
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        #[tokio::test]
        async fn try_for_each_concurrent_control_mut_with_gracefully_ends_when_multiple_functions_return_failure()
        -> Result<(), Box<dyn std::error::Error>> {
            let (mut fn_graph, mut seq_rx) = complex_graph_unit_mut()?;

            let mut resources = Resources::new();
            resources.insert(0u8);
            resources.insert(0u16);
            let resources = &resources;

            let (ControlFlow::Continue(_) | ControlFlow::Break(_)) = test_timeout(
                Duration::from_millis(150),
                Duration::from_millis(197),
                fn_graph.try_for_each_concurrent_control_mut_with(
                    None,
                    StreamOpts::new().rev(),
                    |f| {
                        let fut = f.call_mut(resources);
                        async move {
                            match fut.await {
                                "b" => ControlFlow::Break(()),
                                "c" => ControlFlow::Break(()),
                                _ => ControlFlow::Continue(()),
                            }
                        }
                    },
                ),
            )
            .await;

            assert_eq!("e", seq_rx.try_recv().unwrap());
            assert_eq!("d", seq_rx.try_recv().unwrap());
            assert_eq!("b", seq_rx.try_recv().unwrap());
            assert_eq!("c", seq_rx.try_recv().unwrap());
            assert_eq!(TryRecvError::Empty, seq_rx.try_recv().unwrap_err());

            Ok(())
        }

        async fn test_timeout<T, Fut>(lower: Duration, upper: Duration, fut: Fut) -> T
        where
            Fut: Future<Output = T>,
        {
            let timestamp_begin = Instant::now();
            let t = tokio::time::timeout(Duration::from_millis(750), fut)
                .await
                .unwrap();
            let duration_elapsed = timestamp_begin.elapsed();

            // 6 functions, 50 ms sleep, but two groupings of two functions that may be
            // concurrent
            assert!(
                duration_elapsed < upper,
                "expected duration to be less than {} ms, duration was {} ms",
                upper.as_millis(),
                duration_elapsed.as_millis()
            );
            assert!(
                duration_elapsed > lower,
                "expected duration to be more than {} ms, duration was {} ms",
                lower.as_millis(),
                duration_elapsed.as_millis()
            );

            t
        }

        type BoxFnRes = Box<dyn FnRes<Ret = BoxFuture<'static, &'static str>>>;
        fn complex_graph_unit()
        -> Result<(FnGraph<BoxFnRes>, Receiver<&'static str>), WouldCycle<Edge>> {
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
                    (move |_: &u8| -> BoxFuture<'_, &'static str> {
                        seq_tx_a
                            .try_send("a")
                            .expect("Failed to send sequence `a`.");
                        async {
                            time::sleep(sleep_duration!()).await;
                            "a"
                        }
                        .boxed()
                    })
                    .into_fn_res(),
                    (move |_: &mut u16| -> BoxFuture<'_, &'static str> {
                        seq_tx_b
                            .try_send("b")
                            .expect("Failed to send sequence `b`.");
                        async {
                            time::sleep(sleep_duration!()).await;
                            "b"
                        }
                        .boxed()
                    })
                    .into_fn_res(),
                    (move || -> BoxFuture<'_, &'static str> {
                        seq_tx_c
                            .try_send("c")
                            .expect("Failed to send sequence `c`.");
                        async {
                            time::sleep(sleep_duration!()).await;
                            "c"
                        }
                        .boxed()
                    })
                    .into_fn_res(),
                    (move |_: &u8, _: &mut u16| -> BoxFuture<'_, &'static str> {
                        seq_tx_d
                            .try_send("d")
                            .expect("Failed to send sequence `d`.");
                        async {
                            time::sleep(sleep_duration!()).await;
                            "d"
                        }
                        .boxed()
                    })
                    .into_fn_res(),
                    (move || -> BoxFuture<'_, &'static str> {
                        seq_tx_e
                            .try_send("e")
                            .expect("Failed to send sequence `e`.");
                        async {
                            time::sleep(sleep_duration!()).await;
                            "e"
                        }
                        .boxed()
                    })
                    .into_fn_res(),
                    (move |_: &mut u16| -> BoxFuture<'_, &'static str> {
                        seq_tx_f
                            .try_send("f")
                            .expect("Failed to send sequence `f`.");
                        async {
                            time::sleep(sleep_duration!()).await;
                            "f"
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

        type BoxFnResMut = Box<dyn FnResMut<Ret = BoxFuture<'static, &'static str>>>;

        fn complex_graph_unit_mut()
        -> Result<(FnGraph<BoxFnResMut>, Receiver<&'static str>), WouldCycle<Edge>> {
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
                    (move |_: &u8| -> BoxFuture<&'static str> {
                        if let Some(seq_tx_a) = seq_tx_a.take() {
                            seq_tx_a
                                .try_send("a")
                                .expect("Failed to send sequence `a`.");
                        }
                        Box::pin(async {
                            time::sleep(sleep_duration!()).await;
                            "a"
                        })
                    })
                    .into_fn_res_mut(),
                    (move |_: &mut u16| -> BoxFuture<&'static str> {
                        if let Some(seq_tx_b) = seq_tx_b.take() {
                            seq_tx_b
                                .try_send("b")
                                .expect("Failed to send sequence `b`.");
                        }
                        Box::pin(async {
                            time::sleep(sleep_duration!()).await;
                            "b"
                        })
                    })
                    .into_fn_res_mut(),
                    (move || -> BoxFuture<&'static str> {
                        if let Some(seq_tx_c) = seq_tx_c.take() {
                            seq_tx_c
                                .try_send("c")
                                .expect("Failed to send sequence `c`.");
                        }
                        Box::pin(async {
                            time::sleep(sleep_duration!()).await;
                            "c"
                        })
                    })
                    .into_fn_res_mut(),
                    (move |_: &u8, _: &mut u16| -> BoxFuture<&'static str> {
                        if let Some(seq_tx_d) = seq_tx_d.take() {
                            seq_tx_d
                                .try_send("d")
                                .expect("Failed to send sequence `d`.");
                        }
                        Box::pin(async {
                            time::sleep(sleep_duration!()).await;
                            "d"
                        })
                    })
                    .into_fn_res_mut(),
                    (move || -> BoxFuture<&'static str> {
                        if let Some(seq_tx_e) = seq_tx_e.take() {
                            seq_tx_e
                                .try_send("e")
                                .expect("Failed to send sequence `e`.");
                        }
                        Box::pin(async {
                            time::sleep(sleep_duration!()).await;
                            "e"
                        })
                    })
                    .into_fn_res_mut(),
                    (move |_: &mut u16| -> BoxFuture<&'static str> {
                        if let Some(seq_tx_f) = seq_tx_f.take() {
                            seq_tx_f
                                .try_send("f")
                                .expect("Failed to send sequence `f`.");
                        }
                        Box::pin(async {
                            time::sleep(sleep_duration!()).await;
                            "f"
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

        #[derive(Debug, PartialEq, Eq)]
        struct TestError(&'static str);

        impl fmt::Display for TestError {
            #[cfg_attr(coverage_nightly, coverage(off))]
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl std::error::Error for TestError {}
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
