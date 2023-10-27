use crate::{FnGraphStreamProgress, FnGraphStreamProgressState, FnId};

/// How a `FnGraph` stream operation ended and IDs that were processed.
///
/// Currently this is only constructed by
/// `FnGraphStreamOutcome::from(FnGraphStreamProgress)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FnGraphStreamOutcome {
    /// How a `FnGraph` stream operation ended.
    pub(crate) state: FnGraphStreamOutcomeState,
    /// IDs of the items that were processed.
    pub(crate) fn_ids_processed: Vec<FnId>,
    /// IDs of the items that were not processed.
    pub(crate) fn_ids_not_processed: Vec<FnId>,
}

impl FnGraphStreamOutcome {
    /// Returns a `FnGraphStreamOutcome` that is `Finished`.
    pub fn finished_with(fn_ids_processed: Vec<FnId>) -> Self {
        Self {
            state: FnGraphStreamOutcomeState::Finished,
            fn_ids_processed,
            fn_ids_not_processed: Vec::new(),
        }
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

impl Default for FnGraphStreamOutcome {
    fn default() -> Self {
        Self {
            state: FnGraphStreamOutcomeState::NotStarted,
            fn_ids_processed: Vec::new(),
            fn_ids_not_processed: Vec::new(),
        }
    }
}

impl From<FnGraphStreamProgress> for FnGraphStreamOutcome {
    fn from(fn_graph_stream_progress: FnGraphStreamProgress) -> Self {
        let FnGraphStreamProgress {
            state,
            fn_ids_processed,
            fn_ids_not_processed,
        } = fn_graph_stream_progress;

        Self {
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
