use std::ops::{Deref, Drop};

use tokio::sync::mpsc::Sender;

use crate::FnId;

/// A reference to the function, which also signals when the reference is
/// dropped.
#[derive(Debug)]
pub struct FnRef<'f, F> {
    /// Graph ID of the function this references.
    pub(crate) fn_id: FnId,
    /// Reference to the function.
    pub(crate) r#fn: &'f F,
    /// Channel to notify when this reference is dropped.
    pub(crate) fn_done_tx: Sender<FnId>,
}

impl<'f, F> Deref for FnRef<'f, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        self.r#fn
    }
}

impl<'f, F> Drop for FnRef<'f, F> {
    fn drop(&mut self) {
        self.fn_done_tx.try_send(self.fn_id).unwrap_or_else(|_| {
            panic!(
                "Failed to notify that fn `{}` is complete.",
                self.fn_id.index()
            )
        });
    }
}
