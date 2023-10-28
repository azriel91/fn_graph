/// Order to stream items.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamOrder {
    /// Iterate items in their declared dependency order.
    Forward,
    /// Iterate items beginning from the last item.
    Reverse,
}
