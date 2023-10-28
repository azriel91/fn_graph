use crate::FnId;

/// State during processing a `FnGraph` stream, and the IDs that are processed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamProgress<T> {
    /// The value of the outcome.
    pub(crate) value: T,
    /// State during processing a `FnGraph` stream.
    pub(crate) state: StreamProgressState,
    /// IDs of the items that are processed.
    pub(crate) fn_ids_processed: Vec<FnId>,
    /// IDs of the items that are yet to be processed.
    pub(crate) fn_ids_not_processed: Vec<FnId>,
}

impl<T> StreamProgress<T> {
    /// Returns an empty `FnGraphStreamOutcome`.
    pub fn empty(value: T) -> Self {
        Self {
            value,
            state: StreamProgressState::NotStarted,
            fn_ids_processed: Vec::new(),
            fn_ids_not_processed: Vec::new(),
        }
    }

    /// Returns an empty `FnGraphStreamProgress` with the given capacity.
    pub fn with_capacity(value: T, capacity: usize) -> Self {
        Self {
            value,
            state: StreamProgressState::NotStarted,
            fn_ids_processed: Vec::with_capacity(capacity),
            fn_ids_not_processed: Vec::with_capacity(capacity),
        }
    }

    pub fn into_value(self) -> T {
        self.value
    }

    pub fn value(&self) -> &T {
        &self.value
    }

    pub fn value_mut(&mut self) -> &mut T {
        &mut self.value
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
pub enum StreamProgressState {
    /// The stream is not started, so no items are processed.
    NotStarted,
    /// Whether the stream was interruptible.
    InProgress,
    /// The stream was not interrupted and finished, so all items are
    /// processed.
    Finished,
}
