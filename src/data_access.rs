use crate::TypeIds;

pub use self::{r::R, w::W};

mod r;
mod w;

/// Data accessed by this type.
pub trait DataAccess {
    /// Returns the [`TypeId`]s of borrowed arguments.
    ///
    /// [`TypeId`]: core::any::TypeId
    fn borrows() -> TypeIds
    where
        Self: Sized;

    /// Returns the [`TypeId`]s of mutably borrowed arguments.
    ///
    /// [`TypeId`]: core::any::TypeId
    fn borrow_muts() -> TypeIds
    where
        Self: Sized;
}

/// Data accessed by this type.
pub trait DataAccessDyn {
    /// Returns the [`TypeId`]s of borrowed arguments.
    ///
    /// [`TypeId`]: core::any::TypeId
    fn borrows(&self) -> TypeIds;
    /// Returns the [`TypeId`]s of mutably borrowed arguments.
    ///
    /// [`TypeId`]: core::any::TypeId
    fn borrow_muts(&self) -> TypeIds;
}

#[cfg(not(feature = "fn_meta"))]
impl DataAccess for () {
    fn borrows() -> TypeIds
    where
        Self: Sized,
    {
        TypeIds::new()
    }

    fn borrow_muts() -> TypeIds
    where
        Self: Sized,
    {
        TypeIds::new()
    }
}

#[cfg(not(feature = "fn_meta"))]
impl DataAccessDyn for () {
    fn borrows(&self) -> TypeIds
    where
        Self: Sized,
    {
        TypeIds::new()
    }

    fn borrow_muts(&self) -> TypeIds
    where
        Self: Sized,
    {
        TypeIds::new()
    }
}

#[cfg(not(feature = "fn_meta"))]
impl DataAccess for &() {
    fn borrows() -> TypeIds
    where
        Self: Sized,
    {
        TypeIds::new()
    }

    fn borrow_muts() -> TypeIds
    where
        Self: Sized,
    {
        TypeIds::new()
    }
}

#[cfg(not(feature = "fn_meta"))]
impl DataAccessDyn for &() {
    fn borrows(&self) -> TypeIds
    where
        Self: Sized,
    {
        TypeIds::new()
    }

    fn borrow_muts(&self) -> TypeIds
    where
        Self: Sized,
    {
        TypeIds::new()
    }
}

#[cfg(feature = "fn_meta")]
use fn_meta::{FnMeta, FnMetaDyn};

#[cfg(feature = "fn_meta")]
impl<T> DataAccess for T
where
    T: FnMeta,
{
    fn borrows() -> TypeIds {
        <T as FnMeta>::borrows()
    }

    fn borrow_muts() -> TypeIds {
        <T as FnMeta>::borrow_muts()
    }
}

#[cfg(feature = "fn_meta")]
impl<T> DataAccessDyn for T
where
    T: FnMetaDyn,
{
    fn borrows(&self) -> TypeIds {
        <T as FnMetaDyn>::borrows(self)
    }

    fn borrow_muts(&self) -> TypeIds {
        <T as FnMetaDyn>::borrow_muts(self)
    }
}

/// Borrows data from `Resources`.
#[cfg(feature = "resman")]
pub trait DataBorrow<'borrow> {
    /// Borrows `Self`'s underlying data type from the provided [`Resources`].
    ///
    /// [`Resources`]: resman::Resources
    fn borrow(resources: &'borrow resman::Resources) -> Self;
}

#[cfg(test)]
mod tests {
    use std::any::TypeId;

    use super::{DataAccess, DataAccessDyn};

    #[test]
    fn unit() {
        assert_eq!([] as [TypeId; 0], <() as DataAccess>::borrows().as_slice());
        assert_eq!(
            [] as [TypeId; 0],
            <() as DataAccess>::borrow_muts().as_slice()
        );
        assert_eq!([] as [TypeId; 0], ().borrows().as_slice());
        assert_eq!([] as [TypeId; 0], ().borrow_muts().as_slice());

        assert_eq!([] as [TypeId; 0], <&() as DataAccess>::borrows().as_slice());
        assert_eq!(
            [] as [TypeId; 0],
            <&() as DataAccess>::borrow_muts().as_slice()
        );
        assert_eq!([] as [TypeId; 0], DataAccessDyn::borrows(&&()).as_slice());
        assert_eq!(
            [] as [TypeId; 0],
            DataAccessDyn::borrow_muts(&&()).as_slice()
        );
    }
}
