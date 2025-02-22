use daggy::EdgeIndex;

use crate::FnIdInner;

/// Type alias for edge ID.
pub type EdgeId = EdgeIndex<FnIdInner>;
