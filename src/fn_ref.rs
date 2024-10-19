use std::ops::Deref;

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

impl<F> Deref for FnRef<'_, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        self.r#fn
    }
}

impl<F> Drop for FnRef<'_, F> {
    fn drop(&mut self) {
        // Notify that this function is no longer used.
        // We ignore the result because the receiver may be closed before the reference
        // to the last function is dropped.
        let _ = self.fn_done_tx.try_send(self.fn_id);
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use crate::FnId;

    use super::FnRef;

    #[test]
    fn debug() {
        let (fn_done_tx, _fn_done_rx) = mpsc::channel(1);

        let fn_ref = FnRef {
            fn_id: FnId::new(1),
            r#fn: &(),
            fn_done_tx,
        };

        assert!(format!("{fn_ref:?}")
            .starts_with("FnRef { fn_id: NodeIndex(FnIdInner(1)), fn: (), fn_done_tx: Sender"));
    }
}
