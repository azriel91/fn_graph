/// Number of incoming and outgoing edges.
///
/// This is used during streaming -- a clone of the relevant edge counts is
///  decremented each time.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EdgeCounts {
    /// Number of incoming (parent) edges.
    incoming: Vec<usize>,
    /// Number of outgoing (child) edges.
    outgoing: Vec<usize>,
}

impl EdgeCounts {
    /// Returns new `EdgeCounts`.
    pub fn new(incoming: Vec<usize>, outgoing: Vec<usize>) -> Self {
        Self { incoming, outgoing }
    }

    /// Get a reference to the incoming (parent) counts.
    pub fn incoming(&self) -> &[usize] {
        self.incoming.as_ref()
    }

    /// Get a reference to the outgoing (parent) counts.
    pub fn outgoing(&self) -> &[usize] {
        self.outgoing.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::EdgeCounts;

    #[test]
    fn clone() {
        let edge_counts = EdgeCounts {
            incoming: vec![1, 2],
            outgoing: vec![2, 1],
        };

        assert_eq!(edge_counts, edge_counts.clone());
    }

    #[test]
    fn debug() {
        let edge_counts = EdgeCounts {
            incoming: vec![1, 2],
            outgoing: vec![2, 1],
        };

        assert_eq!(
            "EdgeCounts { incoming: [1, 2], outgoing: [2, 1] }",
            format!("{edge_counts:?}")
        );
    }

    #[test]
    fn partial_eq() {
        let edge_counts_0 = EdgeCounts {
            incoming: vec![1, 2],
            outgoing: vec![2, 1],
        };
        let edge_counts_1 = EdgeCounts {
            incoming: vec![2, 3],
            outgoing: vec![3, 2],
        };

        assert_eq!(edge_counts_0, edge_counts_0.clone());
        assert!(edge_counts_0 != edge_counts_1);
    }
}
