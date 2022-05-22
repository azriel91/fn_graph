use core::any::TypeId;

use smallvec::SmallVec;

/// Array of [`TypeId`]s.
///
/// Holds up to 8 Type IDs before overflowing onto the heap.
pub type TypeIds = SmallVec<[TypeId; 8]>;
