//! Multi-key / composite locking: acquire up to [`MAX_COMPOSITE_KEYS`] keys as
//! one all-or-nothing unit.
//!
//! Built on the single-key engine. Two rules give the semantics:
//!
//! - **Conflict on overlap** — a composite holds *every* one of its component
//!   single-key locks, so two composites that share any key are mutually
//!   exclusive (the single-key quorum guarantees one holder per key).
//! - **Deadlock-free** — components are acquired in a canonical (sorted) order,
//!   so the global "waits-for" relation only points from smaller keys to larger
//!   ones and can never form a cycle.
//!
//! [`Composite`] is a small state machine: ask it which key to [`Composite::pending`]
//! request next, feed each single-key acquisition into [`Composite::on_acquired`],
//! and it tells you when the whole composite is [`Composite::is_held`].

use std::collections::BTreeSet;

use crate::{Fence, LockId};

/// Maximum number of distinct keys in one composite lock.
pub const MAX_COMPOSITE_KEYS: usize = 3;

/// Why a composite key set was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompositeError {
    /// No keys supplied.
    Empty,
    /// More than [`MAX_COMPOSITE_KEYS`] distinct keys.
    TooMany(usize),
    /// A key was the empty string.
    EmptyKey,
}

impl std::fmt::Display for CompositeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompositeError::Empty => write!(f, "composite needs at least one key"),
            CompositeError::TooMany(n) => {
                write!(f, "composite allows at most {MAX_COMPOSITE_KEYS} keys, got {n}")
            }
            CompositeError::EmptyKey => write!(f, "composite keys must be non-empty"),
        }
    }
}

impl std::error::Error for CompositeError {}

/// Canonicalize a composite key set: reject empty keys, de-duplicate, sort, and
/// enforce the 1..=[`MAX_COMPOSITE_KEYS`] count bound.
pub fn canonical_keys(keys: &[String]) -> Result<Vec<LockId>, CompositeError> {
    if keys.iter().any(|k| k.is_empty()) {
        return Err(CompositeError::EmptyKey);
    }
    let set: BTreeSet<&String> = keys.iter().collect();
    if set.is_empty() {
        return Err(CompositeError::Empty);
    }
    if set.len() > MAX_COMPOSITE_KEYS {
        return Err(CompositeError::TooMany(set.len()));
    }
    Ok(set.into_iter().cloned().collect())
}

/// What feeding an acquisition into a [`Composite`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Progress {
    /// Not the key we were waiting on; nothing changed.
    Ignored,
    /// Component acquired; now request this next key.
    Next(LockId),
    /// Every component is held — the composite is acquired.
    Held,
}

/// An ordered, all-or-nothing acquire of up to [`MAX_COMPOSITE_KEYS`] keys.
#[derive(Debug, Clone)]
pub struct Composite {
    keys: Vec<LockId>,
    next: usize,
    fences: Vec<Fence>,
    held: bool,
}

impl Composite {
    /// Build from an arbitrary key list (canonicalized). The first key to
    /// request is [`Composite::pending`].
    pub fn new(keys: &[String]) -> Result<Self, CompositeError> {
        Ok(Composite {
            keys: canonical_keys(keys)?,
            next: 0,
            fences: Vec::new(),
            held: false,
        })
    }

    /// The canonical (sorted, de-duped) component keys.
    pub fn keys(&self) -> &[LockId] {
        &self.keys
    }

    /// The key to request right now — the next un-acquired component — or
    /// `None` once the composite is fully held.
    pub fn pending(&self) -> Option<&LockId> {
        if self.held {
            None
        } else {
            self.keys.get(self.next)
        }
    }

    /// Whether all components are held.
    pub fn is_held(&self) -> bool {
        self.held
    }

    /// The fence token obtained for `key`, if it has been acquired.
    pub fn fence(&self, key: &str) -> Option<Fence> {
        self.keys
            .iter()
            .position(|k| k == key)
            .and_then(|i| self.fences.get(i).copied())
    }

    /// Feed a single-key acquisition. Advances only if it is the component we
    /// are currently waiting on (so stray/duplicate acquisitions are ignored).
    pub fn on_acquired(&mut self, key: &str, fence: Fence) -> Progress {
        if self.held || self.keys.get(self.next).map(String::as_str) != Some(key) {
            return Progress::Ignored;
        }
        self.fences.push(fence);
        self.next += 1;
        match self.keys.get(self.next) {
            Some(k) => Progress::Next(k.clone()),
            None => {
                self.held = true;
                Progress::Held
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(xs: &[&str]) -> Vec<String> {
        xs.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn canonicalize_dedups_sorts_and_caps() {
        assert_eq!(canonical_keys(&s(&["C", "A", "B"])).unwrap(), s(&["A", "B", "C"]));
        assert_eq!(canonical_keys(&s(&["A", "A", "B"])).unwrap(), s(&["A", "B"]));
        assert_eq!(canonical_keys(&s(&["only"])).unwrap(), s(&["only"]));
        assert_eq!(canonical_keys(&s(&[])), Err(CompositeError::Empty));
        assert_eq!(canonical_keys(&s(&["A", ""])), Err(CompositeError::EmptyKey));
        // Four distinct keys exceed the cap; a duplicate that nets to 3 is fine.
        assert_eq!(canonical_keys(&s(&["A", "B", "C", "D"])), Err(CompositeError::TooMany(4)));
        assert_eq!(canonical_keys(&s(&["A", "B", "C", "A"])).unwrap(), s(&["A", "B", "C"]));
    }

    #[test]
    fn composite_advances_in_sorted_order() {
        let mut c = Composite::new(&s(&["B", "A", "C"])).unwrap();
        assert_eq!(c.pending().map(String::as_str), Some("A"));
        assert_eq!(c.on_acquired("A", 1), Progress::Next("B".to_string()));
        assert_eq!(c.on_acquired("B", 2), Progress::Next("C".to_string()));
        assert_eq!(c.on_acquired("C", 3), Progress::Held);
        assert!(c.is_held());
        assert_eq!(c.pending(), None);
        assert_eq!(c.fence("A"), Some(1));
        assert_eq!(c.fence("C"), Some(3));
        // Out-of-order / duplicate acquisitions are ignored.
        assert_eq!(c.on_acquired("A", 9), Progress::Ignored);
    }
}
