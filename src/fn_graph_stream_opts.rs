use std::marker::PhantomData;

#[cfg(feature = "interruptible")]
use interruptible::interruptibility::Interruptibility;

use crate::StreamOrder;

/// How to stream item from the graph.
#[derive(Debug)]
pub struct FnGraphStreamOpts<'rx> {
    /// Order to stream items.
    pub(crate) stream_order: StreamOrder,
    /// Whether the stream is interruptible
    #[cfg(feature = "interruptible")]
    pub(crate) interruptibility: Interruptibility<'rx>,
    /// Marker.
    pub(crate) marker: PhantomData<&'rx ()>,
}

impl<'rx> FnGraphStreamOpts<'rx> {
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

    /// Sets the interruptibility of the stream.
    #[cfg(feature = "interruptible")]
    pub fn interruptibility(mut self, interruptibility: Interruptibility<'rx>) -> Self {
        self.interruptibility = interruptibility;
        self
    }
}

impl<'rx> Default for FnGraphStreamOpts<'rx> {
    fn default() -> Self {
        Self {
            stream_order: StreamOrder::Forward,
            #[cfg(feature = "interruptible")]
            interruptibility: Interruptibility::NonInterruptible,
            marker: PhantomData,
        }
    }
}
