/// Edge between two functions.
///
/// # `Logic` vs `Contains`
///
/// The difference between the `Logic` variant` and the `Contains` variant
/// is whether the non-existence of a predecessor implies the non-existence
/// of the successor.
///
/// For example, a database server may be referenced by an application
/// server, so there is a `Logic`al ordering between the two, but the
/// application server *may* exist even if the database server doesn't.
///
/// However, a file on the application server cannot physically exist if the
/// application server does not exist -- i.e. the application server `Contains`
/// the file.
///
/// This semantic difference can be used for:
///
/// * Cleaning up outermost resources, and assuming the inner resources are also
///   cleaned up.
/// * Rendering.
#[cfg_attr(feature = "graph_info", derive(serde::Deserialize, serde::Serialize))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Edge {
    /// Logical / functional peer (adjacent) dependency.
    ///
    /// The predecessor runs before the successor, and the successor exists
    /// separately to the predecessor.
    Logic,
    /// Logical / functional container dependency.
    ///
    /// The predecessor runs before the successor, and the successor is
    /// contained by the predecessor.
    Contains,
    /// There is a data dependency.
    ///
    /// The function cannot be run because a previous function is accessing data
    /// that this function requires.
    Data,
}

#[cfg(test)]
mod tests {
    use super::Edge;

    #[test]
    fn clone() {
        let edge = Edge::Logic;

        assert_eq!(Edge::Logic, edge.clone());
    }

    #[test]
    fn debug() {
        let edge = Edge::Logic;

        assert_eq!("Logic", format!("{edge:?}"));
    }
}
