use std::{marker::PhantomData, ops::Deref};

/// Wraps the graph `F` function type to upper bind the `'graph` lifetime to the
/// `'iter` lifetime.
///
/// See:
///
/// * <https://users.rust-lang.org/t/102064>
pub struct FnWrapper<'iter, 'graph: 'iter, F> {
    /// The function stored in the graph.
    f: &'iter F,
    /// Marker.
    marker: PhantomData<&'graph ()>,
}

impl<'iter, 'graph: 'iter, F> FnWrapper<'iter, 'graph, F> {
    /// Returns a new `FnWrapper`.
    pub(crate) fn new(f: &'iter F) -> Self {
        Self {
            f,
            marker: PhantomData,
        }
    }
}

impl<F> Deref for FnWrapper<'_, '_, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        self.f
    }
}
