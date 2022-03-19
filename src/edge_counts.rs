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
