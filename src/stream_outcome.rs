use daggy::{petgraph::visit::IntoNodeReferences, Dag};

use crate::{Edge, FnId, FnIdInner};

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

    /// Returns a new `FnGraphStreamOutcome` using values from `StreamProgress`
    /// and the provided `fn_ids_processed`.
    pub fn new(
        graph_structure: &Dag<(), Edge, FnIdInner>,
        value: T,
        stream_outcome_state: StreamOutcomeState,
        fn_ids_processed: Vec<FnId>,
    ) -> Self {
        let fn_ids_not_processed = graph_structure
            .node_references()
            .filter_map(|(fn_id, &())| {
                if fn_ids_processed.contains(&fn_id) {
                    None
                } else {
                    Some(fn_id)
                }
            })
            .collect::<Vec<_>>();

        Self {
            value,
            state: stream_outcome_state,
            fn_ids_processed,
            fn_ids_not_processed,
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

    /// Replaces the value from this outcome with another, taking the current
    /// value as a parameter.
    pub fn replace_with<TNew, U>(self, f: impl FnOnce(T) -> (TNew, U)) -> (StreamOutcome<TNew>, U) {
        let StreamOutcome {
            value,
            state,
            fn_ids_processed,
            fn_ids_not_processed,
        } = self;

        let (value, extracted) = f(value);

        (
            StreamOutcome {
                value,
                state,
                fn_ids_processed,
                fn_ids_not_processed,
            },
            extracted,
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

/// How a `FnGraph` stream operation ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamOutcomeState {
    /// The stream was not started, so no items were processed.
    ///
    /// If a `StreamOutcome` ends with this state, it means the stream was never
    /// polled.
    ///
    /// If a stream is interrupted between the first poll, and that future's
    /// completion, the state will be `StreamOutcomeState::Interrupted`.
    NotStarted,
    /// The stream was interrupted during processing.
    Interrupted,
    /// The stream was not interrupted and finished, so all items were
    /// processed.
    Finished,
}

#[cfg(test)]
mod tests {
    use crate::FnId;

    use super::{StreamOutcome, StreamOutcomeState};

    #[test]
    fn map() {
        let stream_outcome = StreamOutcome::finished_with(1u16, vec![FnId::new(0)]);

        let stream_outcome = stream_outcome.map(|n| n + 1);

        assert_eq!(
            StreamOutcome::finished_with(2u16, vec![FnId::new(0)]),
            stream_outcome
        );
    }

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

    #[test]
    fn replace_with() {
        let stream_outcome =
            StreamOutcome::finished_with((1u16, "value_to_extract"), vec![FnId::new(0)]);

        let (stream_outcome, value) = stream_outcome.replace_with(|(n, value)| (n, value));

        assert_eq!("value_to_extract", value);
        assert_eq!(
            StreamOutcome::finished_with(1u16, vec![FnId::new(0)]),
            stream_outcome
        );
    }

    #[test]
    fn into_value() {
        let stream_outcome = StreamOutcome::finished_with(1u16, vec![FnId::new(0)]);

        let n = stream_outcome.into_value();

        assert_eq!(1u16, n);
    }

    #[test]
    fn value() {
        let stream_outcome = StreamOutcome::finished_with(1u16, vec![FnId::new(0)]);

        let n = stream_outcome.value();

        assert_eq!(1u16, *n);
    }

    #[test]
    fn value_mut() {
        let mut stream_outcome = StreamOutcome::finished_with(1u16, vec![FnId::new(0)]);

        *stream_outcome.value_mut() += 1;

        assert_eq!(2u16, *stream_outcome.value());
    }

    #[test]
    fn state() {
        let stream_outcome = StreamOutcome::finished_with(1u16, vec![FnId::new(0)]);

        assert_eq!(StreamOutcomeState::Finished, stream_outcome.state());
    }

    #[test]
    fn fn_ids_processed() {
        let stream_outcome = StreamOutcome::finished_with(1u16, vec![FnId::new(0)]);

        assert_eq!(&[FnId::new(0)], stream_outcome.fn_ids_processed());
    }

    #[test]
    fn fn_ids_not_processed() {
        let mut stream_outcome = StreamOutcome::finished_with(1u16, vec![FnId::new(0)]);
        stream_outcome.fn_ids_not_processed = vec![FnId::new(1)];

        assert_eq!(&[FnId::new(1)], stream_outcome.fn_ids_not_processed());
    }

    #[test]
    fn default() {
        let stream_outcome = StreamOutcome::<u16>::default();

        assert_eq!(0u16, *stream_outcome.value());
        assert_eq!(StreamOutcomeState::NotStarted, stream_outcome.state());
        assert!(stream_outcome.fn_ids_processed().is_empty());
        assert!(stream_outcome.fn_ids_not_processed().is_empty());
    }
}
