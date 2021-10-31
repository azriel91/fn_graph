use std::ops::{Deref, DerefMut};

use daggy::{petgraph::graph::NodeReferences, Dag, NodeWeightsMut};

use crate::{Edge, FnIdInner};

/// Directed acyclic graph of functions.
#[derive(Clone, Debug)]
pub struct FnGraph<F>(pub Dag<F, Edge, FnIdInner>);

impl<F> FnGraph<F> {
    /// Returns an empty graph of functions.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns an iterator over references of all functions.
    pub fn iter(&self) -> impl Iterator<Item = &F> + ExactSizeIterator + DoubleEndedIterator {
        use daggy::petgraph::visit::IntoNodeReferences;
        self.0
            .node_references()
            .map(|(_, station_spec)| station_spec)
    }

    /// Returns an iterator over mutable references of all functions.
    pub fn iter_mut(&mut self) -> NodeWeightsMut<F, FnIdInner> {
        self.0.node_weights_mut()
    }

    /// Returns an iterator over references of all functions.
    ///
    /// Each iteration returns a `(FnId, &'a F)`.
    pub fn iter_with_indices(&self) -> NodeReferences<F, FnIdInner> {
        use daggy::petgraph::visit::IntoNodeReferences;
        self.0.node_references()
    }
}

impl<F> Default for FnGraph<F> {
    fn default() -> Self {
        Self(Dag::new())
    }
}

impl<F> Deref for FnGraph<F> {
    type Target = Dag<F, Edge, FnIdInner>;

    #[cfg(not(tarpaulin_include))]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<F> DerefMut for FnGraph<F> {
    #[cfg(not(tarpaulin_include))]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
