use daggy::NodeIndex;

use crate::FnIdInner;

/// Type alias for function ID.
pub type FnId = NodeIndex<FnIdInner>;
