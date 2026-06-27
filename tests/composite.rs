//! Multi-key / composite locking over the real engine (driven through the
//! simulator). Verifies, across randomized FIFO interleavings:
//!
//! - **overlap conflict** — two composites sharing any key are never both fully
//!   held at the same time;
//! - **deadlock-free progress** — even a cyclic key-sharing workload
//!   ({A,B},{B,C},{A,C},{A,B,C},...) drives every composite to completion;
//! - **parallelism** — disjoint composites are held simultaneously;
//! - **fencing** — per-key tokens strictly increase across holders;
//! - the **3-key cap** is enforced.

use std::collections::HashMap;

use live_mutex_mills::composite::{Composite, Progress, MAX_COMPOSITE_KEYS};
use live_mutex_mills::sim::Sim;
use live_mutex_mills::{Fence, NodeId};

const HOLD: u32 = 4;

/// One composite job per node. Returns the max number of composites held at the
/// same instant (for the parallelism check).
fn run(n: usize, seed: u64, jobs_spec: &[(NodeId, &[&str])]) -> u32 {
    let mut sim = Sim::with_seed(n, seed);

    let mut jobs: HashMap<NodeId, Composite> = HashMap::new();
    for &(node, keys) in jobs_spec {
        let owned: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        let c = Composite::new(&owned).expect("valid composite");
        sim.request(node, c.pending().unwrap());
        jobs.insert(node, c);
    }

    // Which node currently holds each key (only set while a composite is held).
    let mut key_owner: HashMap<String, NodeId> = HashMap::new();
    // Held composites and remaining hold time.
    let mut hold_left: HashMap<NodeId, u32> = HashMap::new();
    let mut completed: std::collections::HashSet<NodeId> = std::collections::HashSet::new();
    // Per-key observed fence tokens, to assert monotonicity.
    let mut fences: HashMap<String, Vec<Fence>> = HashMap::new();
    let mut max_concurrent = 0u32;

    let total = jobs_spec.len();
    let mut steps = 0u64;

    loop {
        // 1. Feed acquisitions into the owning composite.
        for (node, key, fence) in sim.drain_acquired() {
            fences.entry(key.clone()).or_default().push(fence);
            let job = jobs.get_mut(&node).expect("job for node");
            match job.on_acquired(&key, fence) {
                Progress::Next(next) => sim.request(node, &next),
                Progress::Held => {
                    // Composite fully held: assert no shared key is held elsewhere.
                    for k in job.keys() {
                        assert!(
                            !key_owner.contains_key(k),
                            "COMPOSITE OVERLAP on {k}: node {} holds it while node {node} holds its composite (seed {seed})",
                            key_owner[k],
                        );
                    }
                    for k in job.keys() {
                        key_owner.insert(k.clone(), node);
                    }
                    hold_left.insert(node, HOLD);
                    max_concurrent = max_concurrent.max(hold_left.len() as u32);
                }
                Progress::Ignored => {}
            }
        }

        // 2. Tick down hold timers; release (all keys) when done.
        for node in hold_left.keys().copied().collect::<Vec<_>>() {
            let left = hold_left.get_mut(&node).unwrap();
            if *left == 0 {
                hold_left.remove(&node);
                let job = jobs.get(&node).unwrap();
                for k in job.keys() {
                    key_owner.remove(k);
                    sim.release(node, k);
                }
                completed.insert(node);
            } else {
                *left -= 1;
            }
        }

        // 3. Step, or finish.
        if sim.step() {
            steps += 1;
            assert!(steps < 5_000_000, "composite livelock (seed {seed})");
            continue;
        }
        if hold_left.is_empty() && completed.len() == total {
            break;
        }
        // Idle but unfinished: release any lingering holders to unstick.
        if hold_left.is_empty() {
            // Nothing held and network idle but not all done -> a stuck waiter.
            // Let the loop re-drain; if truly stuck the step cap will fire.
            if sim.drain_acquired().is_empty() {
                // genuinely wedged
                assert_eq!(completed.len(), total, "composite progress stalled (seed {seed})");
                break;
            }
        }
    }

    assert_eq!(completed.len(), total, "all composites should complete (seed {seed})");
    for (key, seq) in &fences {
        for w in seq.windows(2) {
            assert!(w[1] > w[0], "fence not monotonic on {key}: {seq:?} (seed {seed})");
        }
    }
    max_concurrent
}

#[test]
fn cyclic_overlap_is_deadlock_free_and_exclusive() {
    // A,B,C form a sharing cycle; node 3 wants all three; node 4 a single key.
    let spec: &[(NodeId, &[&str])] = &[
        (0, &["A", "B"]),
        (1, &["B", "C"]),
        (2, &["A", "C"]),
        (3, &["C", "B", "A"]), // unsorted on purpose
        (4, &["A"]),
    ];
    for seed in 1..120u64 {
        run(5, seed, spec);
    }
}

#[test]
fn disjoint_composites_run_in_parallel() {
    let spec: &[(NodeId, &[&str])] = &[(0, &["A", "B"]), (1, &["C", "D"]), (2, &["E", "F"])];
    let mut ever_parallel = false;
    for seed in 1..120u64 {
        if run(5, seed, spec) >= 2 {
            ever_parallel = true;
        }
    }
    assert!(ever_parallel, "disjoint composites must be held simultaneously at least once");
}

#[test]
fn three_key_composites_under_contention() {
    let spec: &[(NodeId, &[&str])] = &[
        (0, &["k1", "k2", "k3"]),
        (1, &["k2", "k3", "k4"]),
        (2, &["k3", "k4", "k5"]),
        (3, &["k1", "k5"]),
    ];
    for seed in 1..100u64 {
        run(5, seed, spec);
    }
}

#[test]
fn cap_is_three() {
    let four: Vec<String> = ["a", "b", "c", "d"].iter().map(|s| s.to_string()).collect();
    assert!(Composite::new(&four).is_err());
    assert_eq!(MAX_COMPOSITE_KEYS, 3);
}

#[test]
fn canonical_keys_dedups_and_sorts() {
    use live_mutex_mills::composite::canonical_keys;
    let keys: Vec<String> = ["b", "a", "b", "c", "a"].iter().map(|s| s.to_string()).collect();
    let canon = canonical_keys(&keys).expect("valid after dedup");
    assert_eq!(canon, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    // A composite built from the same multiset acquires in that canonical order — the
    // single global key order that makes overlapping composites deadlock-free.
    let c = Composite::new(&keys).expect("valid composite");
    assert_eq!(c.keys(), canon.as_slice());
    assert_eq!(c.pending().map(String::as_str), Some("a"));
}

#[test]
fn canonical_keys_rejects_empty_set_and_empty_keys() {
    use live_mutex_mills::composite::{canonical_keys, CompositeError};
    assert!(matches!(canonical_keys(&[]), Err(CompositeError::Empty)));
    let with_blank: Vec<String> = ["ok".to_string(), String::new()];
    assert!(matches!(
        canonical_keys(&with_blank),
        Err(CompositeError::EmptyKey)
    ));
}

#[test]
fn duplicate_keys_do_not_count_toward_the_three_key_cap() {
    use live_mutex_mills::composite::canonical_keys;
    // Four entries but only three DISTINCT keys after de-dup -> accepted.
    let dupes: Vec<String> = ["a", "b", "a", "c"].iter().map(|s| s.to_string()).collect();
    assert_eq!(canonical_keys(&dupes).unwrap().len(), 3);
    // Four distinct keys -> rejected as too many.
    let four: Vec<String> = ["a", "b", "c", "d"].iter().map(|s| s.to_string()).collect();
    assert!(canonical_keys(&four).is_err());
}

#[test]
fn composite_of_one_key_behaves_as_a_plain_exclusive_lock() {
    // A single-key "composite" is just a lock: many nodes contending the same one key
    // must stay mutually exclusive and the per-key fence must strictly increase. This
    // proves the composite layer degenerates cleanly to the single-key engine.
    let spec: &[(NodeId, &[&str])] = &[(0, &["solo"]), (1, &["solo"]), (2, &["solo"]), (3, &["solo"])];
    for seed in 1..120u64 {
        run(5, seed, spec);
    }
}
