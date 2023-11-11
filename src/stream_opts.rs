use std::marker::PhantomData;

#[cfg(feature = "interruptible")]
use interruptible::{Interruptibility, InterruptibilityState};

use crate::StreamOrder;

/// How to stream item from the graph.
#[derive(Debug)]
pub struct StreamOpts<'rx, 'intx> {
    /// Order to stream items.
    pub(crate) stream_order: StreamOrder,
    /// Whether the stream is interruptible
    #[cfg(feature = "interruptible")]
    pub(crate) interruptibility_state: InterruptibilityState<'rx, 'intx>,
    /// Marker.
    pub(crate) marker: PhantomData<&'intx &'rx ()>,
}

impl<'rx, 'intx> StreamOpts<'rx, 'intx> {
    /// Returns a new `FnGraphStreamOpts` with forward iteration and
    /// non-interruptibility.
    pub fn new() -> Self {
        Self::default()
    }

    /// Streams items in reverse order.
    ///
    /// Multiple calls to this function will be the same as one call.
    pub fn rev(mut self) -> Self {
        self.stream_order = StreamOrder::Reverse;
        self
    }

    /// Sets the interruptibility_state of the stream.
    #[cfg(feature = "interruptible")]
    pub fn interruptibility_state(
        mut self,
        interruptibility_state: InterruptibilityState<'rx, 'intx>,
    ) -> Self {
        self.interruptibility_state = interruptibility_state;
        self
    }
}

impl<'rx, 'intx> Default for StreamOpts<'rx, 'intx> {
    fn default() -> Self {
        Self {
            stream_order: StreamOrder::Forward,
            #[cfg(feature = "interruptible")]
            interruptibility_state: Interruptibility::NonInterruptible.into(),
            marker: PhantomData,
        }
    }
}
