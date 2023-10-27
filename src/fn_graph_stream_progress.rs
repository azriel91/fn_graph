use crate::FnId;

/// State during processing a `FnGraph` stream, and the IDs that are processed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FnGraphStreamProgress {
    /// State during processing a `FnGraph` stream.
    pub(crate) state: FnGraphStreamProgressState,
    /// IDs of the items that are processed.
    pub(crate) fn_ids_processed: Vec<FnId>,
    /// IDs of the items that are yet to be processed.
    pub(crate) fn_ids_not_processed: Vec<FnId>,
}

impl FnGraphStreamProgress {
    /// Returns an empty `FnGraphStreamOutcome`.
    pub fn empty() -> Self {
        Self {
            state: FnGraphStreamProgressState::NotStarted,
            fn_ids_processed: Vec::new(),
            fn_ids_not_processed: Vec::new(),
        }
    }

    /// Returns an empty `FnGraphStreamProgress` with the given capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            state: FnGraphStreamProgressState::NotStarted,
            fn_ids_processed: Vec::with_capacity(capacity),
            fn_ids_not_processed: Vec::with_capacity(capacity),
        }
    }

    pub fn state(&self) -> FnGraphStreamProgressState {
        self.state
    }

    pub fn fn_ids_processed(&self) -> &[FnId] {
        self.fn_ids_processed.as_ref()
    }

    pub fn fn_ids_not_processed(&self) -> &[FnId] {
        self.fn_ids_not_processed.as_ref()
    }
}

/// State during processing a `FnGraph` stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FnGraphStreamProgressState {
    /// The stream is not started, so no items are processed.
    NotStarted,
    /// Whether the stream was interruptible.
    InProgress,
    /// The stream was not interrupted and finished, so all items are
    /// processed.
    Finished,
}
