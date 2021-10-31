use daggy::petgraph::graph::IndexType;

/// Type safe function ID for a `FnGraph`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FnIdInner(pub usize);

unsafe impl IndexType for FnIdInner {
    #[inline(always)]
    fn new(x: usize) -> Self {
        FnIdInner(x)
    }

    #[inline(always)]
    fn index(&self) -> usize {
        self.0
    }

    #[inline(always)]
    fn max() -> Self {
        FnIdInner(std::usize::MAX)
    }
}
