#[cfg(not(feature = "fn_meta"))]
use core::any::TypeId;

#[cfg(not(feature = "fn_meta"))]
use crate::{DataAccess, TypeIds};

/// Write access to `T`.
#[cfg(not(feature = "resman"))]
#[derive(Debug)]
pub struct W<'write, T>(&'write T);

#[cfg(feature = "resman")]
pub type W<'write, T> = resman::RefMut<'write, T>;

#[cfg(not(feature = "fn_meta"))]
impl<'write, T> DataAccess for W<'write, T>
where
    T: 'static,
{
    fn borrows(&self) -> TypeIds {
        TypeIds::new()
    }

    fn borrow_muts(&self) -> TypeIds {
        let mut type_ids = TypeIds::new();
        type_ids.push(TypeId::of::<T>());
        type_ids
    }
}

/// Borrows data from `Resources`.
#[cfg(feature = "resman")]
impl<'borrow, T> crate::DataBorrow<'borrow> for W<'borrow, T>
where
    T: std::fmt::Debug + Send + Sync + 'static,
{
    fn borrow(resources: &'borrow resman::Resources) -> Self {
        resources.borrow_mut::<T>()
    }
}
