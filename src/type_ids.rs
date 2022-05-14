use core::any::TypeId;

use arrayvec::ArrayVec;

/// Array of [`TypeId`]s.
///
/// Holds a maximum of 8 Type IDs.
pub type TypeIds = ArrayVec<TypeId, 8>;
