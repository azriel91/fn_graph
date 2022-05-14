use core::any::TypeId;

use crate::{DataAccess, TypeIds};

/// Write access to `T`.
#[derive(Debug)]
pub struct W<'write, T>(&'write T);

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
