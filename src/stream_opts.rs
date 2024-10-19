use std::marker::PhantomData;

#[cfg(feature = "interruptible")]
use interruptible::{Interruptibility, InterruptibilityState};

use crate::StreamOrder;

/// How to stream item from the graph.
#[derive(Debug)]
pub struct StreamOpts<'rx, 'intx> {
    /// Order to stream items.
    pub(crate) stream_order: StreamOrder,
    /// Whether the stream is interruptible.
    #[cfg(feature = "interruptible")]
    pub(crate) interruptibility_state: InterruptibilityState<'rx, 'intx>,
    /// Whether the stream should produce the next `FnId` when interrupted.
    ///
    /// Defaults to `true`, so no `FnId`s are dropped if they are polled.
    #[cfg(feature = "interruptible")]
    pub(crate) interrupted_next_item_include: bool,
    /// Marker.
    pub(crate) marker: PhantomData<&'intx &'rx ()>,
}

#[allow(clippy::needless_lifetimes)] // false positive: https://github.com/rust-lang/rust-clippy/issues/12908
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

    /// Whether the stream should produce the next `FnId` when interrupted.
    ///
    /// Defaults to `true`, so no `FnId`s are dropped if they are polled.
    #[cfg(feature = "interruptible")]
    pub fn interrupted_next_item_include(mut self, interrupted_next_item_include: bool) -> Self {
        self.interrupted_next_item_include = interrupted_next_item_include;
        self
    }
}

impl Default for StreamOpts<'_, '_> {
    fn default() -> Self {
        Self {
            stream_order: StreamOrder::Forward,
            #[cfg(feature = "interruptible")]
            interruptibility_state: Interruptibility::NonInterruptible.into(),
            #[cfg(feature = "interruptible")]
            interrupted_next_item_include: true,
            marker: PhantomData,
        }
    }
}
