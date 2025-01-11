use daggy2::NodeIndex;

use crate::FnIdInner;

/// Type alias for function ID.
pub type FnId = NodeIndex<FnIdInner>;
