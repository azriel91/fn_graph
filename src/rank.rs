use std::ops::Add;

/// Rank of a function in the graph.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Rank(pub usize);

impl Add for Rank {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self(self.0 + other.0)
    }
}

impl Add<usize> for Rank {
    type Output = Self;

    fn add(self, other: usize) -> Self {
        Self(self.0 + other)
    }
}

#[cfg(test)]
mod tests {
    use std::{cmp::Ordering, collections::HashMap};

    use super::Rank;

    #[test]
    fn debug() {
        assert_eq!("Rank(1)", format!("{:?}", Rank(1)));
    }

    #[test]
    fn add() {
        assert_eq!(Rank(3), Rank(1) + Rank(2));
    }

    #[test]
    fn add_usize() {
        assert_eq!(Rank(3), Rank(1) + 2);
    }

    #[test]
    fn hash() {
        let mut map = HashMap::new();
        map.insert(Rank(1), 1u32);

        assert_eq!(Some(1u32), map.get(&Rank(1)).copied());
    }

    #[test]
    fn partial_ord() {
        assert!(Rank(1) > Rank(0));
        assert!(Rank(0) < Rank(1));
        assert_eq!(Ordering::Equal, Rank(1).cmp(&Rank(1)));
    }
}
