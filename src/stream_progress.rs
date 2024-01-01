/// State during processing a `FnGraph` stream, and the IDs that are processed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamProgress<T> {
    /// The value of the outcome.
    pub(crate) value: T,
    /// State during processing a `FnGraph` stream.
    pub(crate) state: StreamProgressState,
}

impl<T> StreamProgress<T> {
    /// Returns a new `FnGraphStreamProgress` with IDs of the functions to
    /// process.
    pub fn new(value: T) -> Self {
        Self {
            value,
            state: StreamProgressState::NotStarted,
        }
    }

    /// Returns a new `FnGraphStreamProgress` that is finished.
    pub fn finished_with(value: T) -> Self {
        Self {
            value,
            state: StreamProgressState::Finished,
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
}

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
