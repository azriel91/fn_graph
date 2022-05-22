#[cfg(not(feature = "fn_meta"))]
use core::any::TypeId;

#[cfg(not(feature = "fn_meta"))]
use crate::{DataAccess, TypeIds};

/// Read access to `T`.
#[cfg(not(feature = "resman"))]
#[derive(Debug)]
pub struct R<'read, T>(&'read T);

#[cfg(feature = "resman")]
pub type R<'read, T> = resman::Ref<'read, T>;

#[cfg(not(feature = "fn_meta"))]
impl<'read, T> DataAccess for R<'read, T>
where
    T: 'static,
{
    fn borrows(&self) -> TypeIds {
        let mut type_ids = TypeIds::new();
        type_ids.push(TypeId::of::<T>());
        type_ids
    }

    fn borrow_muts(&self) -> TypeIds {
        TypeIds::new()
    }
}

/// Borrows data from `Resources`.
#[cfg(feature = "resman")]
impl<'borrow, T> crate::DataBorrow<'borrow> for R<'borrow, T>
where
    T: std::fmt::Debug + Send + Sync + 'static,
{
    fn borrow(resources: &'borrow resman::Resources) -> Self {
        resources.borrow::<T>()
    }
}
