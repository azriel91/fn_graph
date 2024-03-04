#[cfg(not(feature = "fn_meta"))]
use core::any::TypeId;

#[cfg(not(feature = "fn_meta"))]
use crate::{DataAccess, DataAccessDyn, TypeIds};

/// Read access to `T`.
#[cfg(not(feature = "resman"))]
#[derive(Debug)]
pub struct R<'read, T>(&'read T);

#[cfg(not(feature = "resman"))]
impl<'read, T> R<'read, T> {
    /// Returns a new `R` wrapper.
    pub fn new(t: &'read T) -> Self {
        Self(t)
    }
}

/// Read access to `T`.
#[cfg(feature = "resman")]
pub type R<'read, T> = resman::Ref<'read, T>;

#[cfg(not(feature = "resman"))]
impl<'read, T> std::ops::Deref for R<'read, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.0
    }
}

#[cfg(not(feature = "fn_meta"))]
impl<'read, T> DataAccess for R<'read, T>
where
    T: 'static,
{
    fn borrows() -> TypeIds {
        let mut type_ids = TypeIds::new();
        type_ids.push(TypeId::of::<T>());
        type_ids
    }

    fn borrow_muts() -> TypeIds {
        TypeIds::new()
    }
}

#[cfg(not(feature = "fn_meta"))]
impl<'read, T> DataAccessDyn for R<'read, T>
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

#[cfg(all(not(feature = "resman"), not(feature = "fn_meta")))]
#[cfg(test)]
mod tests {
    use std::any::TypeId;

    use super::R;
    use crate::{DataAccess, DataAccessDyn};

    #[test]
    fn unit() {
        assert_eq!(
            [TypeId::of::<u32>()] as [TypeId; 1],
            <R<'_, u32> as DataAccess>::borrows().as_slice()
        );
        assert_eq!(
            [] as [TypeId; 0],
            <R<'_, u32> as DataAccess>::borrow_muts().as_slice()
        );
        assert_eq!(
            [TypeId::of::<u32>()] as [TypeId; 1],
            R(&1u32).borrows().as_slice()
        );
        assert_eq!([] as [TypeId; 0], R(&1u32).borrow_muts().as_slice());
    }

    #[test]
    fn debug() {
        let r = R::<'_, u32>(&1);

        assert_eq!("R(1)", format!("{r:?}"));
    }

    #[test]
    fn deref() {
        let n = 1u32;
        let r = R::<'_, u32>::new(&n);

        assert_eq!(1, *r);
    }
}

#[cfg(feature = "resman")]
#[cfg(test)]
mod tests {
    use resman::Resources;

    use super::R;
    use crate::DataBorrow;

    #[test]
    fn unit() {
        let mut resources = Resources::new();
        resources.insert(1u32);

        assert_eq!(1, *R::<'_, u32>::borrow(&resources));
    }

    #[test]
    fn debug() {
        let mut resources = Resources::new();
        resources.insert(1u32);

        let r = R::<'_, u32>::borrow(&resources);

        assert_eq!("Ref { inner: 1 }", format!("{r:?}"));
    }
}
