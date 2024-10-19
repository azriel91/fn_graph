use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

/// Wraps the graph `F` function type to upper bind the `'graph` lifetime to the
/// `'iter` lifetime.
///
/// See:
///
/// * <https://users.rust-lang.org/t/102064>
pub struct FnWrapperMut<'iter, 'graph: 'iter, F> {
    /// The function stored in the graph.
    f: &'iter mut F,
    /// Marker.
    marker: PhantomData<&'graph ()>,
}

impl<'iter, 'graph: 'iter, F> FnWrapperMut<'iter, 'graph, F> {
    /// Returns a new `FnWrapperMut`.
    pub(crate) fn new(f: &'iter mut F) -> Self {
        Self {
            f,
            marker: PhantomData,
        }
    }
}

impl<F> Deref for FnWrapperMut<'_, '_, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        self.f
    }
}

impl<F> DerefMut for FnWrapperMut<'_, '_, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.f
    }
}
