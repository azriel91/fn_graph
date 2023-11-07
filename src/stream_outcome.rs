use crate::{FnId, StreamProgress, StreamProgressState};

/// How a `FnGraph` stream operation ended and IDs that were processed.
///
/// Currently this is only constructed by
/// `FnGraphStreamOutcome::from(FnGraphStreamProgress)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamOutcome<T> {
    /// The value of the outcome.
    pub value: T,
    /// How a `FnGraph` stream operation ended.
    pub state: StreamOutcomeState,
    /// IDs of the items that were processed.
    pub fn_ids_processed: Vec<FnId>,
    /// IDs of the items that were not processed.
    pub fn_ids_not_processed: Vec<FnId>,
}

impl<T> StreamOutcome<T> {
    /// Returns a `FnGraphStreamOutcome` that is `Finished<T>`.
    pub fn finished_with(value: T, fn_ids_processed: Vec<FnId>) -> Self {
        Self {
            value,
            state: StreamOutcomeState::Finished,
            fn_ids_processed,
            fn_ids_not_processed: Vec::new(),
        }
    }

    /// Maps this outcome's value to another.
    pub fn map<TNew>(self, f: impl FnOnce(T) -> TNew) -> StreamOutcome<TNew> {
        let StreamOutcome {
            value,
            state,
            fn_ids_processed,
            fn_ids_not_processed,
        } = self;

        let value = f(value);

        StreamOutcome {
            value,
            state,
            fn_ids_processed,
            fn_ids_not_processed,
        }
    }

    /// Replaces the value from this outcome with another.
    pub fn replace<TNew>(self, value_new: TNew) -> (StreamOutcome<TNew>, T) {
        let StreamOutcome {
            value: value_existing,
            state,
            fn_ids_processed,
            fn_ids_not_processed,
        } = self;

        (
            StreamOutcome {
                value: value_new,
                state,
                fn_ids_processed,
                fn_ids_not_processed,
            },
            value_existing,
        )
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

    pub fn state(&self) -> StreamOutcomeState {
        self.state
    }

    pub fn fn_ids_processed(&self) -> &[FnId] {
        self.fn_ids_processed.as_ref()
    }

    pub fn fn_ids_not_processed(&self) -> &[FnId] {
        self.fn_ids_not_processed.as_ref()
    }
}

impl<T> Default for StreamOutcome<T>
where
    T: Default,
{
    fn default() -> Self {
        Self {
            value: T::default(),
            state: StreamOutcomeState::NotStarted,
            fn_ids_processed: Vec::new(),
            fn_ids_not_processed: Vec::new(),
        }
    }
}

impl<T> From<StreamProgress<T>> for StreamOutcome<T> {
    fn from(fn_graph_stream_progress: StreamProgress<T>) -> Self {
        let StreamProgress {
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
pub enum StreamOutcomeState {
    /// The stream was not started, so no items were processed.
    NotStarted,
    /// The stream was interrupted during processing.
    Interrupted,
    /// The stream was not interrupted and finished, so all items were
    /// processed.
    Finished,
}

impl From<StreamProgressState> for StreamOutcomeState {
    fn from(fn_graph_stream_progress_state: StreamProgressState) -> Self {
        match fn_graph_stream_progress_state {
            StreamProgressState::NotStarted => Self::NotStarted,
            StreamProgressState::InProgress => Self::Interrupted,
            StreamProgressState::Finished => Self::Finished,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::FnId;

    use super::StreamOutcome;

    #[test]
    fn replace() {
        let stream_outcome = StreamOutcome::finished_with(1u16, vec![FnId::new(0)]);

        let (stream_outcome, n) = stream_outcome.replace(2u32);

        assert_eq!(1u16, n);
        assert_eq!(
            StreamOutcome::finished_with(2u32, vec![FnId::new(0)]),
            stream_outcome
        );
    }
}
