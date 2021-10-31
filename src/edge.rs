/// Edge between two functions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Edge {
    /// Logical / functional dependency.
    ///
    /// The function must be run after the previous as requested by the library
    /// consumer.
    Logic,
    /// There is a data dependency.
    ///
    /// The function cannot be run because a previous function is accessing data
    /// that this function requires.
    Data,
}
