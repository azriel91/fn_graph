#[cfg(not(feature = "fn_meta"))]
use core::any::TypeId;

#[cfg(not(feature = "fn_meta"))]
use crate::{DataAccess, DataAccessDyn, TypeIds};

/// Write access to `T`.
#[cfg(not(feature = "resman"))]
#[derive(Debug)]
pub struct W<'write, T>(&'write mut T);

/// Write access to `T`.
#[cfg(feature = "resman")]
pub type W<'write, T> = resman::RefMut<'write, T>;

#[cfg(not(feature = "resman"))]
impl<'write, T> std::ops::Deref for W<'write, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

#[cfg(not(feature = "resman"))]
impl<'write, T> std::ops::DerefMut for W<'write, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

#[cfg(not(feature = "fn_meta"))]
impl<'write, T> DataAccess for W<'write, T>
where
    T: 'static,
{
    fn borrows() -> TypeIds {
        TypeIds::new()
    }

    fn borrow_muts() -> TypeIds {
        let mut type_ids = TypeIds::new();
        type_ids.push(TypeId::of::<T>());
        type_ids
    }
}

#[cfg(not(feature = "fn_meta"))]
impl<'read, T> DataAccessDyn for W<'read, T>
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

#[cfg(all(not(feature = "resman"), not(feature = "fn_meta")))]
#[cfg(test)]
mod tests {
    use std::any::TypeId;

    use super::W;
    use crate::{DataAccess, DataAccessDyn};

    #[test]
    fn unit() {
        assert_eq!(
            [] as [TypeId; 0],
            <W<'_, u32> as DataAccess>::borrows().as_slice()
        );
        assert_eq!(
            [TypeId::of::<u32>()] as [TypeId; 1],
            <W<'_, u32> as DataAccess>::borrow_muts().as_slice()
        );
        assert_eq!([] as [TypeId; 0], W(&mut 1u32).borrows().as_slice());
        assert_eq!(
            [TypeId::of::<u32>()] as [TypeId; 1],
            W(&mut 1u32).borrow_muts().as_slice()
        );
    }

    #[test]
    fn debug() {
        let mut n = 1;
        let w = W::<'_, u32>(&mut n);

        assert_eq!("W(1)", format!("{w:?}"));
    }
}

#[cfg(feature = "resman")]
#[cfg(test)]
mod tests {
    use resman::Resources;

    use super::W;
    use crate::DataBorrow;

    #[test]
    fn unit() {
        let mut resources = Resources::new();
        resources.insert(1u32);

        assert_eq!(1, *W::<'_, u32>::borrow(&resources));
    }

    #[test]
    fn debug() {
        let mut resources = Resources::new();
        resources.insert(1u32);

        let w = W::<'_, u32>::borrow(&resources);

        assert_eq!("RefMut { inner: 1 }", format!("{w:?}"));
    }
}
