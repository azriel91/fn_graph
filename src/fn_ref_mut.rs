use std::ops::{Deref, DerefMut, Drop};

use tokio::sync::mpsc::Sender;

use crate::FnId;

/// A reference to the function, which also signals when the reference is
/// dropped.
#[derive(Debug)]
pub struct FnRefMut<'f, F> {
    /// Graph ID of the function this references.
    pub(crate) fn_id: FnId,
    /// Reference to the function.
    pub(crate) r#fn: &'f mut F,
    /// Channel to notify when this reference is dropped.
    pub(crate) fn_done_tx: Sender<FnId>,
}

impl<'f, F> Deref for FnRefMut<'f, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        self.r#fn
    }
}

impl<'f, F> DerefMut for FnRefMut<'f, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.r#fn
    }
}

impl<'f, F> Drop for FnRefMut<'f, F> {
    fn drop(&mut self) {
        // Notify that this function is no longer used.
        // We ignore the result because the receiver may be closed before the reference
        // to the last function is dropped.
        let _ = self.fn_done_tx.try_send(self.fn_id);
    }
}
