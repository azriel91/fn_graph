use core::any::TypeId;

use crate::{DataAccess, TypeIds};

/// Read access to `T`.
#[derive(Debug)]
pub struct R<'read, T>(&'read T);

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
