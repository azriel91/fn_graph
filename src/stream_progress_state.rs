/// State during processing a `FnGraph` stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamProgressState {
    /// The stream is not started, so no items are processed.
    NotStarted,
    /// The stream is in progress, at least one item is processed.
    InProgress,
    /// The stream was not interrupted and finished, so all items are
    /// processed.
    Finished,
}
