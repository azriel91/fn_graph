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

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::FnIdInner;

    #[test]
    fn debug() {
        assert_eq!("FnIdInner(1)", format!("{:?}", FnIdInner(1)));
    }

    #[test]
    fn default() {
        assert_eq!(FnIdInner(0), FnIdInner::default());
    }

    #[test]
    fn partial_ord() {
        assert!(FnIdInner(1) > FnIdInner(0));
        assert!(FnIdInner(0) < FnIdInner(1));
        assert_eq!(Ordering::Equal, FnIdInner(1).cmp(&FnIdInner(1)));
    }
}
