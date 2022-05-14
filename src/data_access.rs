use crate::TypeIds;

pub use self::{r::R, w::W};

mod r;
mod w;

/// Data read by this type.
pub trait DataAccess {
    /// Returns the [`TypeId`]s of borrowed arguments.
    ///
    /// [`TypeId`]: core::any::TypeId
    fn borrows(&self) -> TypeIds;
    /// Returns the [`TypeId`]s of mutably borrowed arguments.
    ///
    /// [`TypeId`]: core::any::TypeId
    fn borrow_muts(&self) -> TypeIds;
}

#[cfg(feature = "fn_meta")]
use fn_meta::FnMeta;

#[cfg(feature = "fn_meta")]
impl<T> DataAccess for T
where
    T: FnMeta,
{
    fn borrows(&self) -> TypeIds {
        <T as FnMeta>::borrows(self)
    }

    fn borrow_muts(&self) -> TypeIds {
        <T as FnMeta>::borrow_muts(self)
    }
}