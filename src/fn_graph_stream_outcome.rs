use crate::{FnGraphStreamProgress, FnGraphStreamProgressState, FnId};

/// How a `FnGraph` stream operation ended and IDs that were processed.
///
/// Currently this is only constructed by
/// `FnGraphStreamOutcome::from(FnGraphStreamProgress)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FnGraphStreamOutcome<T> {
    /// The value of the outcome.
    pub value: T,
    /// How a `FnGraph` stream operation ended.
    pub state: FnGraphStreamOutcomeState,
    /// IDs of the items that were processed.
    pub fn_ids_processed: Vec<FnId>,
    /// IDs of the items that were not processed.
    pub fn_ids_not_processed: Vec<FnId>,
}

impl<T> FnGraphStreamOutcome<T> {
    /// Returns a `FnGraphStreamOutcome` that is `Finished<T>`.
    pub fn finished_with(value: T, fn_ids_processed: Vec<FnId>) -> Self {
        Self {
            value,
            state: FnGraphStreamOutcomeState::Finished,
            fn_ids_processed,
            fn_ids_not_processed: Vec::new(),
        }
    }

    /// Maps this outcome's value to another.
    pub fn map<TNew>(self, f: impl FnOnce(T) -> TNew) -> FnGraphStreamOutcome<TNew> {
        let FnGraphStreamOutcome {
            value,
            state,
            fn_ids_processed,
            fn_ids_not_processed,
        } = self;

        let value = f(value);

        FnGraphStreamOutcome {
            value,
            state,
            fn_ids_processed,
            fn_ids_not_processed,
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

    pub fn state(&self) -> FnGraphStreamOutcomeState {
        self.state
    }

    pub fn fn_ids_processed(&self) -> &[FnId] {
        self.fn_ids_processed.as_ref()
    }

    pub fn fn_ids_not_processed(&self) -> &[FnId] {
        self.fn_ids_not_processed.as_ref()
    }
}

impl<T> Default for FnGraphStreamOutcome<T>
where
    T: Default,
{
    fn default() -> Self {
        Self {
            value: T::default(),
            state: FnGraphStreamOutcomeState::NotStarted,
            fn_ids_processed: Vec::new(),
            fn_ids_not_processed: Vec::new(),
        }
    }
}

impl<T> From<FnGraphStreamProgress<T>> for FnGraphStreamOutcome<T> {
    fn from(fn_graph_stream_progress: FnGraphStreamProgress<T>) -> Self {
        let FnGraphStreamProgress {
            value,
            state,
            fn_ids_processed,
            fn_ids_not_processed,
        } = fn_graph_stream_progress;

        Self {
            value,
            state: state.into(),
            fn_ids_processed,
            fn_ids_not_processed,
        }
    }
}

/// How a `FnGraph` stream operation ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FnGraphStreamOutcomeState {
    /// The stream was not started, so no items were processed.
    NotStarted,
    /// The stream was interrupted during processing.
    Interrupted,
    /// The stream was not interrupted and finished, so all items were
    /// processed.
    Finished,
}

impl From<FnGraphStreamProgressState> for FnGraphStreamOutcomeState {
    fn from(fn_graph_stream_progress_state: FnGraphStreamProgressState) -> Self {
        match fn_graph_stream_progress_state {
            FnGraphStreamProgressState::NotStarted => Self::NotStarted,
            FnGraphStreamProgressState::InProgress => Self::Interrupted,
            FnGraphStreamProgressState::Finished => Self::Finished,
        }
    }
}
