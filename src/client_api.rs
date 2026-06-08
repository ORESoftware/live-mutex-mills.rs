//! Language-neutral client frontends for requesting locks from an `lmxd` node.
//!
//! The cluster's peer traffic still uses [`crate::transport`]'s FIFO TCP mesh.
//! This module exposes a separate client surface that any language can use:
//! blocking HTTP requests with JSON responses, or a simple line-oriented TCP
//! protocol. Both surfaces feed the same coordinator so concurrent clients on
//! one node are queued and each successful holder gets its own fence token.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use serde::Serialize;
use serde_json::{json, Value};

use crate::transport::{Command, NodeEvent};
use crate::{Fence, LockId};

const MAX_HTTP_BODY: usize = 1024 * 1024;
const MIN_SEMAPHORE_LIMIT: u32 = 2;
const MAX_SEMAPHORE_LIMIT: u32 = 100;
/// Maximum length of a single request/header line (bounds memory per client).
const MAX_LINE: usize = 16 * 1024;
/// Maximum number of HTTP request headers accepted.
const MAX_HTTP_HEADERS: usize = 100;
/// Read/write timeout applied to client sockets (mitigates slowloris/idle).
const CLIENT_IO_TIMEOUT: Duration = Duration::from_secs(30);
/// Cap on concurrent client connections per node (bounds thread/memory use).
const MAX_CLIENT_CONNECTIONS: u64 = 1024;

static ACTIVE_CLIENT_CONNECTIONS: AtomicU64 = AtomicU64::new(0);

/// Read one line, erroring if it exceeds `max` bytes so an unbounded line
/// cannot exhaust memory. Returns the number of bytes read (0 = EOF).
fn read_line_limited<R: BufRead>(reader: &mut R, buf: &mut String, max: usize) -> io::Result<usize> {
    let start = buf.len();
    let n = reader.by_ref().take(max as u64 + 1).read_line(buf)?;
    if buf.len() - start > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "line exceeds maximum length",
        ));
    }
    Ok(n)
}

/// Apply read/write timeouts so a slow or idle client cannot pin a thread.
fn configure_client_socket(stream: &TcpStream) {
    let _ = stream.set_read_timeout(Some(CLIENT_IO_TIMEOUT));
    let _ = stream.set_write_timeout(Some(CLIENT_IO_TIMEOUT));
}

/// RAII guard bounding concurrent client connections; `None` when at capacity.
struct ConnectionGuard;

impl ConnectionGuard {
    fn try_acquire() -> Option<Self> {
        if ACTIVE_CLIENT_CONNECTIONS.fetch_add(1, Ordering::SeqCst) >= MAX_CLIENT_CONNECTIONS {
            ACTIVE_CLIENT_CONNECTIONS.fetch_sub(1, Ordering::SeqCst);
            None
        } else {
            Some(ConnectionGuard)
        }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        ACTIVE_CLIENT_CONNECTIONS.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A successful client-facing lock acquisition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ClientAcquire {
    pub lock: LockId,
    pub fence: Fence,
    pub holder: String,
}

/// A successful client-facing semaphore permit acquisition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ClientSemaphoreAcquire {
    pub semaphore: String,
    pub limit: u32,
    pub permit: u32,
    pub fence: Fence,
    pub holder: String,
}

/// Which side of an RW lock was acquired.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RwMode {
    Read,
    Write,
}

impl RwMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

/// A successful client-facing RW lock acquisition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ClientRwAcquire {
    pub rwlock: String,
    pub mode: RwMode,
    pub limit: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permit: Option<u32>,
    pub fence: Fence,
    pub holder: String,
}

/// A successful client-facing release.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ClientRelease {
    pub lock: LockId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub semaphore: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rwlock: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<RwMode>,
}

impl ClientRelease {
    fn released_text(&self) -> String {
        if let (Some(name), Some(mode), Some(limit)) = (&self.rwlock, self.mode, self.limit) {
            return match (mode, self.permit) {
                (RwMode::Read, Some(permit)) => {
                    format!("RWRELEASED {name} mode=read limit={limit} permit={permit}")
                }
                _ => format!("RWRELEASED {name} mode={} limit={limit}", mode.as_str()),
            };
        }
        match (&self.semaphore, self.limit, self.permit) {
            (Some(name), Some(limit), Some(permit)) => {
                format!("SEMRELEASED {name} limit={limit} permit={permit}")
            }
            _ => format!("RELEASED {}", self.lock),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientError {
    Stopped,
    EmptyLock,
    BadLimit,
    NotHeld,
    HolderMismatch,
}

impl ClientError {
    fn status(&self) -> u16 {
        match self {
            Self::Stopped => 503,
            Self::EmptyLock => 400,
            Self::BadLimit => 400,
            Self::NotHeld => 404,
            Self::HolderMismatch => 409,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            Self::Stopped => "stopped",
            Self::EmptyLock => "empty_lock",
            Self::BadLimit => "bad_limit",
            Self::NotHeld => "not_held",
            Self::HolderMismatch => "holder_mismatch",
        }
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stopped => write!(f, "node stopped"),
            Self::EmptyLock => write!(f, "lock must not be empty"),
            Self::BadLimit => write!(
                f,
                "limit must be between {MIN_SEMAPHORE_LIMIT} and {MAX_SEMAPHORE_LIMIT}"
            ),
            Self::NotHeld => write!(f, "holder token does not refer to a held lock"),
            Self::HolderMismatch => write!(f, "holder token does not own that lock"),
        }
    }
}

impl std::error::Error for ClientError {}

type WaiterId = u64;
type AcquireReply = Sender<(WaiterId, Result<ClientAcquire, ClientError>)>;
type ReleaseReply = Sender<Result<ClientRelease, ClientError>>;

#[derive(Clone, Debug, PartialEq, Eq)]
enum ClientResource {
    Lock {
        lock: LockId,
    },
    Semaphore {
        name: String,
        limit: u32,
        permit: u32,
    },
}

impl ClientResource {
    fn client_lock(&self) -> LockId {
        match self {
            Self::Lock { lock } => lock.clone(),
            Self::Semaphore { name, .. } => name.clone(),
        }
    }

    fn release(&self) -> ClientRelease {
        match self {
            Self::Lock { lock } => ClientRelease {
                lock: lock.clone(),
                semaphore: None,
                limit: None,
                permit: None,
                rwlock: None,
                mode: None,
            },
            Self::Semaphore {
                name,
                limit,
                permit,
            } => ClientRelease {
                lock: name.clone(),
                semaphore: Some(name.clone()),
                limit: Some(*limit),
                permit: Some(*permit),
                rwlock: None,
                mode: None,
            },
        }
    }
}

#[derive(Clone, Debug)]
struct HeldLock {
    lock: LockId,
    holder: String,
}

#[derive(Clone, Debug)]
struct RwBundle {
    name: String,
    limit: u32,
    mode: RwMode,
    permit: Option<u32>,
    parts: Vec<HeldLock>,
}

impl RwBundle {
    fn release(&self) -> ClientRelease {
        ClientRelease {
            lock: self.name.clone(),
            semaphore: None,
            limit: Some(self.limit),
            permit: self.permit,
            rwlock: Some(self.name.clone()),
            mode: Some(self.mode),
        }
    }
}

enum ClientOp {
    Acquire {
        lock: LockId,
        waiter: WaiterId,
        resource: ClientResource,
        reply: AcquireReply,
    },
    CancelAcquire {
        lock: LockId,
        waiter: WaiterId,
    },
    Release {
        lock: Option<LockId>,
        holder: String,
        reply: ReleaseReply,
    },
}

#[derive(Clone)]
pub struct LockClient {
    tx: Sender<ClientOp>,
    next_waiter: Arc<AtomicU64>,
    next_rw_holder: Arc<AtomicU64>,
    rw_holders: Arc<Mutex<HashMap<String, RwBundle>>>,
}

impl LockClient {
    /// Queue for a lock and block until this node holds it.
    pub fn acquire(&self, lock: impl Into<LockId>) -> Result<ClientAcquire, ClientError> {
        let lock = lock.into();
        if lock.is_empty() {
            return Err(ClientError::EmptyLock);
        }
        let (reply, rx) = mpsc::channel();
        self.enqueue_acquire(lock.clone(), ClientResource::Lock { lock }, reply)?;
        rx.recv()
            .map(|(_, result)| result)
            .unwrap_or(Err(ClientError::Stopped))
    }

    /// Queue for one permit from a fixed-size semaphore.
    ///
    /// A semaphore is represented as `limit` independent exclusive permit locks.
    /// Callers that use the same `(name, limit)` contend for the same bounded
    /// resource; changing the limit creates a distinct semaphore namespace.
    pub fn acquire_semaphore(
        &self,
        name: impl Into<String>,
        limit: u32,
    ) -> Result<ClientSemaphoreAcquire, ClientError> {
        let name = name.into();
        validate_bounded_resource(&name, limit)?;

        let (reply, rx) = mpsc::channel();
        let mut pending = HashMap::<WaiterId, (u32, LockId)>::new();
        for permit in 0..limit {
            let lock = semaphore_lock_name(&name, limit, permit);
            let resource = ClientResource::Semaphore {
                name: name.clone(),
                limit,
                permit,
            };
            match self.enqueue_acquire(lock.clone(), resource, reply.clone()) {
                Ok(waiter) => {
                    pending.insert(waiter, (permit, lock));
                }
                Err(e) => {
                    for (waiter, (_, lock)) in pending {
                        self.cancel_acquire(lock, waiter);
                    }
                    return Err(e);
                }
            }
        }
        drop(reply);

        loop {
            match rx.recv() {
                Ok((waiter, Ok(acquired))) => {
                    let Some((permit, _)) = pending.remove(&waiter) else {
                        let _ = self.release(acquired.holder);
                        continue;
                    };
                    for (other, (_, lock)) in pending {
                        self.cancel_acquire(lock, other);
                    }
                    return Ok(ClientSemaphoreAcquire {
                        semaphore: name,
                        limit,
                        permit,
                        fence: acquired.fence,
                        holder: acquired.holder,
                    });
                }
                Ok((waiter, Err(e))) => {
                    pending.remove(&waiter);
                    if pending.is_empty() {
                        return Err(e);
                    }
                }
                Err(_) => return Err(ClientError::Stopped),
            }
        }
    }

    /// Queue for a shared read side of an RW lock.
    ///
    /// The `limit` is the maximum number of concurrent readers. Readers and
    /// writers must use the same `(name, limit)` pair to contend in the same
    /// RW lock namespace.
    pub fn acquire_read(
        &self,
        name: impl Into<String>,
        limit: u32,
    ) -> Result<ClientRwAcquire, ClientError> {
        let name = name.into();
        validate_bounded_resource(&name, limit)?;

        let gate_lock = rw_gate_lock_name(&name, limit);
        let gate = self.acquire(gate_lock.clone())?;
        let slot = self.acquire_rw_slot(&name, limit);
        let gate_release = self.release_for_lock(Some(gate_lock), gate.holder);

        let (permit, slot_lock, slot_acquired) = match (slot, gate_release) {
            (Ok(slot), Ok(_)) => slot,
            (Ok((_, slot_lock, slot_acquired)), Err(e)) => {
                let _ = self.release_for_lock(Some(slot_lock), slot_acquired.holder);
                return Err(e);
            }
            (Err(e), Ok(_)) => return Err(e),
            (Err(e), Err(_)) => return Err(e),
        };

        let holder = self.register_rw_bundle(RwBundle {
            name: name.clone(),
            limit,
            mode: RwMode::Read,
            permit: Some(permit),
            parts: vec![HeldLock {
                lock: slot_lock,
                holder: slot_acquired.holder,
            }],
        });

        Ok(ClientRwAcquire {
            rwlock: name,
            mode: RwMode::Read,
            limit,
            permit: Some(permit),
            fence: slot_acquired.fence,
            holder,
        })
    }

    /// Queue for the exclusive write side of an RW lock.
    ///
    /// A writer holds the admission gate plus every reader permit slot, so no
    /// reader can be active while the writer is held. Once a writer gets the
    /// gate, later readers queue behind it instead of continuously refilling
    /// free reader slots.
    pub fn acquire_write(
        &self,
        name: impl Into<String>,
        limit: u32,
    ) -> Result<ClientRwAcquire, ClientError> {
        let name = name.into();
        validate_bounded_resource(&name, limit)?;

        let gate_lock = rw_gate_lock_name(&name, limit);
        let gate = self.acquire(gate_lock.clone())?;
        let mut parts = Vec::with_capacity(limit as usize + 1);
        let mut fence = gate.fence;

        for permit in 0..limit {
            let lock = rw_slot_lock_name(&name, limit, permit);
            match self.acquire(lock.clone()) {
                Ok(acquired) => {
                    fence = fence.max(acquired.fence);
                    parts.push(HeldLock {
                        lock,
                        holder: acquired.holder,
                    });
                }
                Err(e) => {
                    for part in parts.into_iter().rev() {
                        let _ = self.release_for_lock(Some(part.lock), part.holder);
                    }
                    let _ = self.release_for_lock(Some(gate_lock), gate.holder);
                    return Err(e);
                }
            }
        }

        parts.push(HeldLock {
            lock: gate_lock,
            holder: gate.holder,
        });

        let holder = self.register_rw_bundle(RwBundle {
            name: name.clone(),
            limit,
            mode: RwMode::Write,
            permit: None,
            parts,
        });

        Ok(ClientRwAcquire {
            rwlock: name,
            mode: RwMode::Write,
            limit,
            permit: None,
            fence,
            holder,
        })
    }

    /// Release a lock by the holder token returned from [`Self::acquire`].
    pub fn release(&self, holder: impl Into<String>) -> Result<ClientRelease, ClientError> {
        let holder = holder.into();
        if let Some(bundle) = self.take_rw_bundle(&holder, None)? {
            return self.release_rw_bundle(bundle);
        }
        self.release_for_lock(None, holder)
    }

    /// Release a lock by holder token, additionally checking the lock name.
    pub fn release_lock(
        &self,
        lock: impl Into<LockId>,
        holder: impl Into<String>,
    ) -> Result<ClientRelease, ClientError> {
        self.release_for_lock(Some(lock.into()), holder)
    }

    /// Release an RW lock by holder token, additionally checking the RW lock name.
    pub fn release_rw_lock(
        &self,
        name: impl Into<String>,
        holder: impl Into<String>,
    ) -> Result<ClientRelease, ClientError> {
        let name = name.into();
        let holder = holder.into();
        let Some(bundle) = self.take_rw_bundle(&holder, Some(&name))? else {
            return Err(ClientError::NotHeld);
        };
        self.release_rw_bundle(bundle)
    }

    fn acquire_rw_slot(
        &self,
        name: &str,
        limit: u32,
    ) -> Result<(u32, LockId, ClientAcquire), ClientError> {
        let (reply, rx) = mpsc::channel();
        let mut pending = HashMap::<WaiterId, (u32, LockId)>::new();
        for permit in 0..limit {
            let lock = rw_slot_lock_name(name, limit, permit);
            match self.enqueue_acquire(
                lock.clone(),
                ClientResource::Lock { lock: lock.clone() },
                reply.clone(),
            ) {
                Ok(waiter) => {
                    pending.insert(waiter, (permit, lock));
                }
                Err(e) => {
                    for (waiter, (_, lock)) in pending {
                        self.cancel_acquire(lock, waiter);
                    }
                    return Err(e);
                }
            }
        }
        drop(reply);

        loop {
            match rx.recv() {
                Ok((waiter, Ok(acquired))) => {
                    let Some((permit, lock)) = pending.remove(&waiter) else {
                        let _ = self.release_for_lock(Some(acquired.lock.clone()), acquired.holder);
                        continue;
                    };
                    for (other, (_, lock)) in pending {
                        self.cancel_acquire(lock, other);
                    }
                    return Ok((permit, lock, acquired));
                }
                Ok((waiter, Err(e))) => {
                    pending.remove(&waiter);
                    if pending.is_empty() {
                        return Err(e);
                    }
                }
                Err(_) => return Err(ClientError::Stopped),
            }
        }
    }

    fn register_rw_bundle(&self, bundle: RwBundle) -> String {
        let holder = format!(
            "rw{:016x}",
            self.next_rw_holder.fetch_add(1, Ordering::Relaxed)
        );
        self.rw_holders
            .lock()
            .expect("rw holder map poisoned")
            .insert(holder.clone(), bundle);
        holder
    }

    fn take_rw_bundle(
        &self,
        holder: &str,
        lock: Option<&str>,
    ) -> Result<Option<RwBundle>, ClientError> {
        let mut holders = self.rw_holders.lock().expect("rw holder map poisoned");
        let Some(bundle) = holders.get(holder) else {
            return Ok(None);
        };
        if lock.is_some_and(|want| want != bundle.name) {
            return Err(ClientError::HolderMismatch);
        }
        Ok(holders.remove(holder))
    }

    fn release_rw_bundle(&self, bundle: RwBundle) -> Result<ClientRelease, ClientError> {
        let mut first_error = None;
        for part in &bundle.parts {
            if let Err(e) = self.release_for_lock(Some(part.lock.clone()), part.holder.clone()) {
                first_error.get_or_insert(e);
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(bundle.release()),
        }
    }

    fn release_for_lock(
        &self,
        lock: Option<LockId>,
        holder: impl Into<String>,
    ) -> Result<ClientRelease, ClientError> {
        let holder = holder.into();
        let (reply, rx) = mpsc::channel();
        self.tx
            .send(ClientOp::Release {
                lock,
                holder,
                reply,
            })
            .map_err(|_| ClientError::Stopped)?;
        rx.recv().unwrap_or(Err(ClientError::Stopped))
    }

    fn enqueue_acquire(
        &self,
        lock: LockId,
        resource: ClientResource,
        reply: AcquireReply,
    ) -> Result<WaiterId, ClientError> {
        let waiter = self.next_waiter.fetch_add(1, Ordering::Relaxed);
        self.tx
            .send(ClientOp::Acquire {
                lock,
                waiter,
                resource,
                reply,
            })
            .map_err(|_| ClientError::Stopped)?;
        Ok(waiter)
    }

    fn cancel_acquire(&self, lock: LockId, waiter: WaiterId) {
        let _ = self.tx.send(ClientOp::CancelAcquire { lock, waiter });
    }
}

#[derive(Default)]
struct LockQueue {
    waiters: VecDeque<Waiter>,
    acquiring: bool,
    holder: Option<Holder>,
}

struct Waiter {
    id: WaiterId,
    resource: ClientResource,
    reply: AcquireReply,
}

struct Holder {
    token: String,
    waiter: WaiterId,
    resource: ClientResource,
}

/// Start the client coordinator.
///
/// It consumes raw [`NodeEvent`]s from the transport, manages client waiters,
/// and forwards a clone of every event to the returned receiver for logging or
/// demos.
pub fn start_coordinator(
    cmd_tx: Sender<Command>,
    evt_rx: Receiver<NodeEvent>,
) -> (LockClient, Receiver<NodeEvent>) {
    let (client_tx, client_rx) = mpsc::channel();
    let (public_evt_tx, public_evt_rx) = mpsc::channel();

    thread::spawn(move || coordinator_loop(cmd_tx, evt_rx, client_rx, public_evt_tx));

    (
        LockClient {
            tx: client_tx,
            next_waiter: Arc::new(AtomicU64::new(1)),
            next_rw_holder: Arc::new(AtomicU64::new(1)),
            rw_holders: Arc::new(Mutex::new(HashMap::new())),
        },
        public_evt_rx,
    )
}

fn coordinator_loop(
    cmd_tx: Sender<Command>,
    evt_rx: Receiver<NodeEvent>,
    client_rx: Receiver<ClientOp>,
    public_evt_tx: Sender<NodeEvent>,
) {
    let mut locks: HashMap<LockId, LockQueue> = HashMap::new();
    let mut holders: HashMap<String, LockId> = HashMap::new();
    let mut waiter_holders: HashMap<WaiterId, String> = HashMap::new();
    let mut next_holder = 1u64;

    loop {
        while let Ok(ev) = evt_rx.try_recv() {
            handle_node_event(
                ev,
                &cmd_tx,
                &public_evt_tx,
                &mut locks,
                &mut holders,
                &mut waiter_holders,
                &mut next_holder,
            );
        }

        match client_rx.recv_timeout(Duration::from_millis(50)) {
            Ok(ClientOp::Acquire {
                lock,
                waiter,
                resource,
                reply,
            }) => {
                if lock.is_empty() {
                    let _ = reply.send((waiter, Err(ClientError::EmptyLock)));
                    continue;
                }
                let queue = locks.entry(lock.clone()).or_default();
                queue.waiters.push_back(Waiter {
                    id: waiter,
                    resource,
                    reply,
                });
                if queue.holder.is_none() && !queue.acquiring {
                    queue.acquiring = true;
                    let _ = cmd_tx.send(Command::Acquire(lock));
                }
            }
            Ok(ClientOp::CancelAcquire { lock, waiter }) => cancel_acquire(
                &lock,
                waiter,
                &cmd_tx,
                &mut locks,
                &mut holders,
                &mut waiter_holders,
            ),
            Ok(ClientOp::Release {
                lock,
                holder,
                reply,
            }) => {
                let result = release_holder(
                    lock,
                    &holder,
                    &cmd_tx,
                    &mut locks,
                    &mut holders,
                    &mut waiter_holders,
                );
                let _ = reply.send(result);
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
}

fn handle_node_event(
    ev: NodeEvent,
    cmd_tx: &Sender<Command>,
    public_evt_tx: &Sender<NodeEvent>,
    locks: &mut HashMap<LockId, LockQueue>,
    holders: &mut HashMap<String, LockId>,
    waiter_holders: &mut HashMap<WaiterId, String>,
    next_holder: &mut u64,
) {
    match &ev {
        NodeEvent::Acquired(lock, fence) => {
            if let Some(queue) = locks.get_mut(lock) {
                queue.acquiring = false;
                grant_next_waiter(
                    lock,
                    *fence,
                    queue,
                    holders,
                    waiter_holders,
                    next_holder,
                    cmd_tx,
                );
            }
        }
        NodeEvent::Lost(lock) => {
            if let Some(queue) = locks.get_mut(lock) {
                if let Some(holder) = queue.holder.take() {
                    holders.remove(&holder.token);
                    waiter_holders.remove(&holder.waiter);
                }
                if !queue.waiters.is_empty() && !queue.acquiring {
                    queue.acquiring = true;
                    let _ = cmd_tx.send(Command::Acquire(lock.clone()));
                }
            }
        }
        NodeEvent::Info(_) => {}
    }
    let _ = public_evt_tx.send(ev);
}

fn grant_next_waiter(
    lock: &str,
    fence: Fence,
    queue: &mut LockQueue,
    holders: &mut HashMap<String, LockId>,
    waiter_holders: &mut HashMap<WaiterId, String>,
    next_holder: &mut u64,
    cmd_tx: &Sender<Command>,
) {
    while let Some(waiter) = queue.waiters.pop_front() {
        let token = format!("h{:016x}", *next_holder);
        *next_holder += 1;
        let acquired = ClientAcquire {
            lock: waiter.resource.client_lock(),
            fence,
            holder: token.clone(),
        };
        if waiter.reply.send((waiter.id, Ok(acquired))).is_ok() {
            holders.insert(token.clone(), lock.to_string());
            waiter_holders.insert(waiter.id, token.clone());
            queue.holder = Some(Holder {
                token,
                waiter: waiter.id,
                resource: waiter.resource,
            });
            return;
        }
    }

    // Nobody is still waiting for this acquisition; do not keep an orphaned
    // cluster lock alive.
    let _ = cmd_tx.send(Command::Release(lock.to_string()));
}

fn release_holder(
    lock: Option<LockId>,
    holder: &str,
    cmd_tx: &Sender<Command>,
    locks: &mut HashMap<LockId, LockQueue>,
    holders: &mut HashMap<String, LockId>,
    waiter_holders: &mut HashMap<WaiterId, String>,
) -> Result<ClientRelease, ClientError> {
    let held_lock = holders.get(holder).cloned().ok_or(ClientError::NotHeld)?;
    if lock.as_ref().is_some_and(|want| want != &held_lock) {
        return Err(ClientError::HolderMismatch);
    }

    let Some(queue) = locks.get_mut(&held_lock) else {
        holders.remove(holder);
        return Err(ClientError::NotHeld);
    };
    let resource = match &queue.holder {
        Some(current) if current.token == holder => current.resource.clone(),
        Some(_) => return Err(ClientError::HolderMismatch),
        None => {
            holders.remove(holder);
            return Err(ClientError::NotHeld);
        }
    };

    queue.holder = None;
    holders.remove(holder);
    waiter_holders.retain(|_, token| token != holder);
    let _ = cmd_tx.send(Command::Release(held_lock.clone()));
    if !queue.waiters.is_empty() && !queue.acquiring {
        queue.acquiring = true;
        let _ = cmd_tx.send(Command::Acquire(held_lock.clone()));
    }
    Ok(resource.release())
}

fn cancel_acquire(
    lock: &str,
    waiter: WaiterId,
    cmd_tx: &Sender<Command>,
    locks: &mut HashMap<LockId, LockQueue>,
    holders: &mut HashMap<String, LockId>,
    waiter_holders: &mut HashMap<WaiterId, String>,
) {
    if let Some(queue) = locks.get_mut(lock) {
        if let Some(pos) = queue.waiters.iter().position(|queued| queued.id == waiter) {
            queue.waiters.remove(pos);
            return;
        }
    }

    if let Some(holder) = waiter_holders.get(&waiter).cloned() {
        let _ = release_holder(None, &holder, cmd_tx, locks, holders, waiter_holders);
    }
}

/// Start the language-neutral line-oriented TCP client API.
///
/// Text commands:
///
/// - `ACQUIRE <lock>` blocks until acquired and returns
///   `ACQUIRED <lock> fence=<n> holder=<token>`.
/// - `SEMACQUIRE <limit> <name>` blocks until one semaphore permit is held.
/// - `RACQUIRE <limit> <name>` acquires the shared side of an RW lock.
/// - `WACQUIRE <limit> <name>` acquires the exclusive side of an RW lock.
/// - `RELEASE <holder>` returns `RELEASED <lock>` or `SEMRELEASED ...`.
/// - `PING` returns `PONG`; `QUIT` closes the connection.
///
/// JSON-lines commands are also accepted:
/// `{"op":"acquire","lock":"name"}`,
/// `{"op":"semaphore_acquire","name":"api","limit":10}`,
/// `{"op":"rw_acquire","name":"doc","mode":"read","limit":10}`, and
/// `{"op":"release","holder":"token"}`.
pub fn serve_tcp(addr: &str, client: LockClient) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr)?;
    let local_addr = listener.local_addr()?;
    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let Some(guard) = ConnectionGuard::try_acquire() else {
                continue; // at capacity: drop the connection
            };
            configure_client_socket(&stream);
            let client = client.clone();
            thread::spawn(move || {
                let _guard = guard;
                let _ = handle_tcp_client(stream, client);
            });
        }
    });
    Ok(local_addr)
}

fn handle_tcp_client(mut stream: TcpStream, client: LockClient) -> io::Result<()> {
    writeln!(
        stream,
        "# live-mutex-mills client API: ACQUIRE <lock> | SEMACQUIRE <limit> <name> | RACQUIRE <limit> <name> | WACQUIRE <limit> <name> | RELEASE <holder> | JSON lines"
    )?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    loop {
        line.clear();
        let n = match read_line_limited(&mut reader, &mut line, MAX_LINE) {
            Ok(n) => n,
            Err(_) => {
                let _ = writeln!(stream, "ERR line_too_long");
                return Ok(());
            }
        };
        if n == 0 {
            break; // EOF
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.eq_ignore_ascii_case("quit") || trimmed.eq_ignore_ascii_case("exit") {
            writeln!(stream, "BYE")?;
            return Ok(());
        }
        if trimmed.eq_ignore_ascii_case("ping") {
            writeln!(stream, "PONG")?;
            continue;
        }
        let json_mode = trimmed.starts_with('{');
        let response = if json_mode {
            handle_tcp_json(trimmed, &client)
        } else {
            handle_tcp_text(trimmed, &client)
        };
        writeln!(stream, "{response}")?;
        stream.flush()?;
    }
    Ok(())
}

fn handle_tcp_text(line: &str, client: &LockClient) -> String {
    let (op, rest) = split_word(line);
    match op.to_ascii_lowercase().as_str() {
        "acquire" => {
            let lock = rest.trim();
            match client.acquire(lock.to_string()) {
                Ok(a) => format!("ACQUIRED {} fence={} holder={}", a.lock, a.fence, a.holder),
                Err(e) => format!("ERROR {} {}", e.code(), e),
            }
        }
        "semacquire" | "semaphore" | "acquire-semaphore" => {
            let (limit, name) = split_word(rest.trim());
            let limit = limit.parse::<u32>().ok();
            match limit {
                Some(limit) => match client.acquire_semaphore(name.trim().to_string(), limit) {
                    Ok(a) => format!(
                        "SEMACQUIRED {} limit={} permit={} fence={} holder={}",
                        a.semaphore, a.limit, a.permit, a.fence, a.holder
                    ),
                    Err(e) => format!("ERROR {} {}", e.code(), e),
                },
                None => format!(
                    "ERROR {} {}",
                    ClientError::BadLimit.code(),
                    ClientError::BadLimit
                ),
            }
        }
        "racquire" | "rlock" | "readlock" | "rwread" => {
            let (limit, name) = split_word(rest.trim());
            let limit = limit.parse::<u32>().ok();
            match limit {
                Some(limit) => match client.acquire_read(name.trim().to_string(), limit) {
                    Ok(a) => rw_acquired_text(a),
                    Err(e) => format!("ERROR {} {}", e.code(), e),
                },
                None => format!(
                    "ERROR {} {}",
                    ClientError::BadLimit.code(),
                    ClientError::BadLimit
                ),
            }
        }
        "wacquire" | "wlock" | "writelock" | "rwwrite" => {
            let (limit, name) = split_word(rest.trim());
            let limit = limit.parse::<u32>().ok();
            match limit {
                Some(limit) => match client.acquire_write(name.trim().to_string(), limit) {
                    Ok(a) => rw_acquired_text(a),
                    Err(e) => format!("ERROR {} {}", e.code(), e),
                },
                None => format!(
                    "ERROR {} {}",
                    ClientError::BadLimit.code(),
                    ClientError::BadLimit
                ),
            }
        }
        "release" => {
            let (first, rest) = split_word(rest.trim());
            let holder = if rest.trim().is_empty() {
                first
            } else {
                rest.trim()
            };
            match client.release(holder.to_string()) {
                Ok(r) => r.released_text(),
                Err(e) => format!("ERROR {} {}", e.code(), e),
            }
        }
        _ => "ERROR bad_request use ACQUIRE <lock> | SEMACQUIRE <limit> <name> | RACQUIRE <limit> <name> | WACQUIRE <limit> <name> | RELEASE <holder>"
            .to_string(),
    }
}

fn handle_tcp_json(line: &str, client: &LockClient) -> String {
    let response = match serde_json::from_str::<Value>(line) {
        Ok(v) => match v.get("op").and_then(Value::as_str) {
            Some("acquire") => match v.get("lock").and_then(Value::as_str) {
                Some(lock) => client
                    .acquire(lock.to_string())
                    .map(|a| json!({"ok":true,"lock":a.lock,"fence":a.fence,"holder":a.holder})),
                None => Err(ClientError::EmptyLock),
            },
            Some("semaphore_acquire") | Some("sem_acquire") | Some("semaphore") => {
                let name = v
                    .get("name")
                    .or_else(|| v.get("semaphore"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let limit = v.get("limit").and_then(Value::as_u64).unwrap_or(0);
                match u32::try_from(limit) {
                    Ok(limit) => client.acquire_semaphore(name.to_string(), limit).map(|a| {
                        json!({
                            "ok":true,
                            "semaphore":a.semaphore,
                            "limit":a.limit,
                            "permit":a.permit,
                            "fence":a.fence,
                            "holder":a.holder
                        })
                    }),
                    Err(_) => Err(ClientError::BadLimit),
                }
            }
            Some("rw_acquire") | Some("rwlock_acquire") => {
                let name = rw_name_from_json(&v);
                let mode = v.get("mode").and_then(Value::as_str).unwrap_or("");
                let limit = v.get("limit").and_then(Value::as_u64).unwrap_or(0);
                acquire_rw_json(client, mode, name, limit)
            }
            Some("rw_read_acquire") | Some("rwlock_read_acquire") | Some("read_acquire") => {
                let name = rw_name_from_json(&v);
                let limit = v.get("limit").and_then(Value::as_u64).unwrap_or(0);
                acquire_rw_json(client, "read", name, limit)
            }
            Some("rw_write_acquire") | Some("rwlock_write_acquire") | Some("write_acquire") => {
                let name = rw_name_from_json(&v);
                let limit = v.get("limit").and_then(Value::as_u64).unwrap_or(0);
                acquire_rw_json(client, "write", name, limit)
            }
            Some("release") => match v.get("holder").and_then(Value::as_str) {
                Some(holder) => client.release(holder.to_string()).map(release_json),
                None => Err(ClientError::NotHeld),
            },
            _ => Ok(json!({"ok":false,"error":"bad_request","message":"unknown op"})),
        },
        Err(_) => Ok(json!({"ok":false,"error":"bad_json","message":"malformed JSON line"})),
    };

    match response {
        Ok(v) => v.to_string(),
        Err(e) => json!({"ok":false,"error":e.code(),"message":e.to_string()}).to_string(),
    }
}

fn split_word(s: &str) -> (&str, &str) {
    let trimmed = s.trim_start();
    match trimmed.find(char::is_whitespace) {
        Some(idx) => (&trimmed[..idx], &trimmed[idx..]),
        None => (trimmed, ""),
    }
}

fn validate_bounded_resource(name: &str, limit: u32) -> Result<(), ClientError> {
    if name.is_empty() {
        return Err(ClientError::EmptyLock);
    }
    if !(MIN_SEMAPHORE_LIMIT..=MAX_SEMAPHORE_LIMIT).contains(&limit) {
        return Err(ClientError::BadLimit);
    }
    Ok(())
}

fn semaphore_lock_name(name: &str, limit: u32, permit: u32) -> LockId {
    format!(
        "__lmx_sem_v1:{limit}:{permit}:{}",
        hex_encode(name.as_bytes())
    )
}

fn rw_gate_lock_name(name: &str, limit: u32) -> LockId {
    format!("__lmx_rw_v1:{limit}:gate:{}", hex_encode(name.as_bytes()))
}

fn rw_slot_lock_name(name: &str, limit: u32, permit: u32) -> LockId {
    format!(
        "__lmx_rw_v1:{limit}:slot:{permit}:{}",
        hex_encode(name.as_bytes())
    )
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// Start the HTTP/JSON client API.
///
/// Supported endpoints:
///
/// - `POST /locks/{lock}/acquire`
/// - `POST /locks/{lock}/release` with `{"holder":"..."}` or
///   `X-LMX-Holder: ...`
/// - `POST /locks/acquire` with `{"lock":"..."}`
/// - `POST /locks/release` with `{"holder":"..."}`
/// - `POST /semaphores/{name}/acquire` with `{"limit":10}`
/// - `POST /semaphores/acquire` with `{"name":"api","limit":10}`
/// - `POST /semaphores/release` with `{"holder":"..."}`
/// - `POST /rwlocks/{name}/read-acquire` with `{"limit":10}`
/// - `POST /rwlocks/{name}/write-acquire` with `{"limit":10}`
/// - `POST /rwlocks/acquire` with `{"name":"doc","mode":"read","limit":10}`
/// - `POST /rwlocks/release` with `{"holder":"..."}`
/// - `GET /healthz`
pub fn serve_http(addr: &str, client: LockClient) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr)?;
    let local_addr = listener.local_addr()?;
    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let Some(guard) = ConnectionGuard::try_acquire() else {
                continue; // at capacity: drop the connection
            };
            configure_client_socket(&stream);
            let client = client.clone();
            thread::spawn(move || {
                let _guard = guard;
                let _ = handle_http_client(stream, client);
            });
        }
    });
    Ok(local_addr)
}

fn handle_http_client(mut stream: TcpStream, client: LockClient) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    if read_line_limited(&mut reader, &mut request_line, MAX_LINE)? == 0 {
        return Ok(());
    }
    let mut headers = HashMap::new();
    loop {
        if headers.len() >= MAX_HTTP_HEADERS {
            return write_http_json(
                &mut stream,
                431,
                json!({"ok":false,"error":"too_many_headers"}),
            );
        }
        let mut line = String::new();
        if read_line_limited(&mut reader, &mut line, MAX_LINE)? == 0 {
            return Ok(());
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            break;
        }
        if let Some((name, value)) = trimmed.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }

    let content_len = headers
        .get("content-length")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    if content_len > MAX_HTTP_BODY {
        return write_http_json(
            &mut stream,
            413,
            json!({"ok":false,"error":"body_too_large"}),
        );
    }
    let mut body = vec![0u8; content_len];
    reader.read_exact(&mut body)?;
    let body_json = if body.is_empty() {
        Value::Null
    } else {
        match serde_json::from_slice::<Value>(&body) {
            Ok(v) => v,
            Err(_) => {
                return write_http_json(
                    &mut stream,
                    400,
                    json!({"ok":false,"error":"bad_json","message":"malformed JSON body"}),
                )
            }
        }
    };

    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let target = parts.next().unwrap_or("");
    let (path, query) = target.split_once('?').unwrap_or((target, ""));

    let (status, payload) = route_http(method, path, query, &headers, &body_json, &client);
    write_http_json(&mut stream, status, payload)
}

fn route_http(
    method: &str,
    path: &str,
    query: &str,
    headers: &HashMap<String, String>,
    body: &Value,
    client: &LockClient,
) -> (u16, Value) {
    if method == "GET" && path == "/healthz" {
        return (200, json!({"ok":true}));
    }

    if method != "POST" && method != "DELETE" {
        return (
            405,
            json!({"ok":false,"error":"method_not_allowed","message":"use POST"}),
        );
    }

    if method == "POST" && path == "/locks/acquire" {
        let lock = body.get("lock").and_then(Value::as_str).unwrap_or("");
        return acquire_http(lock, client);
    }
    if (method == "POST" || method == "DELETE") && path == "/locks/release" {
        let holder = holder_from_request(query, headers, body);
        return release_http(None, holder.as_deref(), client);
    }
    if method == "POST" && path == "/semaphores/acquire" {
        let name = body
            .get("name")
            .or_else(|| body.get("semaphore"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let limit = body.get("limit").and_then(Value::as_u64).unwrap_or(0);
        return acquire_semaphore_http(name, limit, client);
    }
    if (method == "POST" || method == "DELETE") && path == "/semaphores/release" {
        let holder = holder_from_request(query, headers, body);
        return release_http(None, holder.as_deref(), client);
    }
    if method == "POST" && path == "/rwlocks/acquire" {
        let name = rw_name_from_json(body);
        let mode = body.get("mode").and_then(Value::as_str).unwrap_or("");
        let limit = body.get("limit").and_then(Value::as_u64).unwrap_or(0);
        return acquire_rw_http(mode, name, limit, client);
    }
    if (method == "POST" || method == "DELETE") && path == "/rwlocks/release" {
        let holder = holder_from_request(query, headers, body);
        return release_rw_http(None, holder.as_deref(), client);
    }

    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
    if segments.len() == 3 && segments[0] == "locks" && segments[2] == "acquire" {
        let lock = percent_decode(segments[1]).unwrap_or_default();
        return acquire_http(&lock, client);
    }
    if segments.len() == 3 && segments[0] == "locks" && segments[2] == "release" {
        let lock = percent_decode(segments[1]).unwrap_or_default();
        let holder = holder_from_request(query, headers, body);
        return release_http(Some(lock), holder.as_deref(), client);
    }
    if segments.len() == 3 && segments[0] == "semaphores" && segments[2] == "acquire" {
        let name = percent_decode(segments[1]).unwrap_or_default();
        let limit = body.get("limit").and_then(Value::as_u64).unwrap_or(0);
        return acquire_semaphore_http(&name, limit, client);
    }
    if segments.len() == 3 && segments[0] == "semaphores" && segments[2] == "release" {
        let holder = holder_from_request(query, headers, body);
        return release_http(None, holder.as_deref(), client);
    }
    if segments.len() == 3
        && segments[0] == "rwlocks"
        && matches!(segments[2], "read-acquire" | "read")
    {
        let name = percent_decode(segments[1]).unwrap_or_default();
        let limit = body.get("limit").and_then(Value::as_u64).unwrap_or(0);
        return acquire_rw_http("read", &name, limit, client);
    }
    if segments.len() == 3
        && segments[0] == "rwlocks"
        && matches!(segments[2], "write-acquire" | "write")
    {
        let name = percent_decode(segments[1]).unwrap_or_default();
        let limit = body.get("limit").and_then(Value::as_u64).unwrap_or(0);
        return acquire_rw_http("write", &name, limit, client);
    }
    if segments.len() == 3 && segments[0] == "rwlocks" && segments[2] == "release" {
        let name = percent_decode(segments[1]).unwrap_or_default();
        let holder = holder_from_request(query, headers, body);
        return release_rw_http(Some(name), holder.as_deref(), client);
    }

    (
        404,
        json!({"ok":false,"error":"not_found","message":"unknown endpoint"}),
    )
}

fn acquire_http(lock: &str, client: &LockClient) -> (u16, Value) {
    match client.acquire(lock.to_string()) {
        Ok(a) => (
            200,
            json!({"ok":true,"lock":a.lock,"fence":a.fence,"holder":a.holder}),
        ),
        Err(e) => (
            e.status(),
            json!({"ok":false,"error":e.code(),"message":e.to_string()}),
        ),
    }
}

fn acquire_semaphore_http(name: &str, limit: u64, client: &LockClient) -> (u16, Value) {
    let result = match u32::try_from(limit) {
        Ok(limit) => client.acquire_semaphore(name.to_string(), limit),
        Err(_) => Err(ClientError::BadLimit),
    };
    match result {
        Ok(a) => (
            200,
            json!({
                "ok":true,
                "semaphore":a.semaphore,
                "limit":a.limit,
                "permit":a.permit,
                "fence":a.fence,
                "holder":a.holder
            }),
        ),
        Err(e) => (
            e.status(),
            json!({"ok":false,"error":e.code(),"message":e.to_string()}),
        ),
    }
}

fn rw_acquired_text(a: ClientRwAcquire) -> String {
    match (a.mode, a.permit) {
        (RwMode::Read, Some(permit)) => format!(
            "RWACQUIRED {} mode=read limit={} permit={} fence={} holder={}",
            a.rwlock, a.limit, permit, a.fence, a.holder
        ),
        _ => format!(
            "RWACQUIRED {} mode={} limit={} fence={} holder={}",
            a.rwlock,
            a.mode.as_str(),
            a.limit,
            a.fence,
            a.holder
        ),
    }
}

fn rw_name_from_json(v: &Value) -> &str {
    v.get("name")
        .or_else(|| v.get("rwlock"))
        .and_then(Value::as_str)
        .unwrap_or("")
}

fn acquire_rw_json(
    client: &LockClient,
    mode: &str,
    name: &str,
    limit: u64,
) -> Result<Value, ClientError> {
    let limit = u32::try_from(limit).map_err(|_| ClientError::BadLimit)?;
    match mode {
        "read" | "r" | "shared" => client.acquire_read(name.to_string(), limit),
        "write" | "w" | "exclusive" => client.acquire_write(name.to_string(), limit),
        _ => Err(ClientError::BadLimit),
    }
    .map(|a| {
        let mut v = json!({
            "ok":true,
            "rwlock":a.rwlock,
            "mode":a.mode,
            "limit":a.limit,
            "fence":a.fence,
            "holder":a.holder
        });
        if let Some(permit) = a.permit {
            v["permit"] = json!(permit);
        }
        v
    })
}

fn acquire_rw_http(mode: &str, name: &str, limit: u64, client: &LockClient) -> (u16, Value) {
    match acquire_rw_json(client, mode, name, limit) {
        Ok(v) => (200, v),
        Err(e) => (
            e.status(),
            json!({"ok":false,"error":e.code(),"message":e.to_string()}),
        ),
    }
}

fn release_http(lock: Option<LockId>, holder: Option<&str>, client: &LockClient) -> (u16, Value) {
    let Some(holder) = holder else {
        return (
            400,
            json!({"ok":false,"error":"missing_holder","message":"release requires a holder token"}),
        );
    };
    let result = match lock {
        Some(lock) => client.release_lock(lock, holder.to_string()),
        None => client.release(holder.to_string()),
    };
    match result {
        Ok(r) => (200, release_json(r)),
        Err(e) => (
            e.status(),
            json!({"ok":false,"error":e.code(),"message":e.to_string()}),
        ),
    }
}

fn release_rw_http(
    lock: Option<LockId>,
    holder: Option<&str>,
    client: &LockClient,
) -> (u16, Value) {
    let Some(holder) = holder else {
        return (
            400,
            json!({"ok":false,"error":"missing_holder","message":"release requires a holder token"}),
        );
    };
    let result = match lock {
        Some(lock) => client.release_rw_lock(lock, holder.to_string()),
        None => client.release(holder.to_string()),
    };
    match result {
        Ok(r) => (200, release_json(r)),
        Err(e) => (
            e.status(),
            json!({"ok":false,"error":e.code(),"message":e.to_string()}),
        ),
    }
}

fn release_json(r: ClientRelease) -> Value {
    if let (Some(rwlock), Some(mode), Some(limit)) = (r.rwlock, r.mode, r.limit) {
        let mut v = json!({
            "ok":true,
            "rwlock":rwlock,
            "mode":mode,
            "limit":limit
        });
        if let Some(permit) = r.permit {
            v["permit"] = json!(permit);
        }
        return v;
    }
    match (r.semaphore, r.limit, r.permit) {
        (Some(semaphore), Some(limit), Some(permit)) => {
            json!({"ok":true,"semaphore":semaphore,"limit":limit,"permit":permit})
        }
        _ => json!({"ok":true,"lock":r.lock}),
    }
}

fn holder_from_request(
    query: &str,
    headers: &HashMap<String, String>,
    body: &Value,
) -> Option<String> {
    body.get("holder")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| headers.get("x-lmx-holder").cloned())
        .or_else(|| query_param(query, "holder").and_then(|v| percent_decode(&v)))
}

fn query_param(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&').filter(|s| !s.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        if percent_decode(k).as_deref() == Some(key) {
            return Some(v.to_string());
        }
    }
    None
}

fn write_http_json(stream: &mut TcpStream, status: u16, payload: Value) -> io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        413 => "Payload Too Large",
        503 => "Service Unavailable",
        _ => "Error",
    };
    let body = payload.to_string();
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len()
    )
}

fn percent_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                if i + 2 >= bytes.len() {
                    return None;
                }
                let hi = hex_value(bytes[i + 1])?;
                let lo = hex_value(bytes[i + 2])?;
                out.push((hi << 4) | lo);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

fn hex_value(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_decodes_paths_and_query_values() {
        assert_eq!(
            percent_decode("tenant%207%2Freport"),
            Some("tenant 7/report".into())
        );
        assert_eq!(percent_decode("bad%xx"), None);
    }

    #[test]
    fn text_release_accepts_legacy_lock_then_holder_shape() {
        let (op, rest) = split_word("release lock-A holder-B");
        assert_eq!(op, "release");
        let (first, second) = split_word(rest.trim());
        assert_eq!(first, "lock-A");
        assert_eq!(second.trim(), "holder-B");
    }

    #[test]
    fn read_line_limited_rejects_overlong_lines() {
        // A line within the limit is read normally.
        let mut ok = BufReader::new(&b"short line\nnext\n"[..]);
        let mut buf = String::new();
        assert!(read_line_limited(&mut ok, &mut buf, 64).is_ok());
        assert_eq!(buf, "short line\n");

        // A line longer than the limit is rejected instead of buffered.
        let huge = format!("{}\n", "x".repeat(10_000));
        let mut over = BufReader::new(huge.as_bytes());
        let mut buf = String::new();
        let err = read_line_limited(&mut over, &mut buf, 1024).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        // EOF returns 0.
        let mut empty = BufReader::new(&b""[..]);
        let mut buf = String::new();
        assert_eq!(read_line_limited(&mut empty, &mut buf, 64).unwrap(), 0);
    }

    #[test]
    fn connection_guard_caps_concurrency() {
        // Exhaust the global limit, then confirm further acquisitions fail and
        // releasing (drop) restores capacity. Runs in isolation via the counter.
        ACTIVE_CLIENT_CONNECTIONS.store(MAX_CLIENT_CONNECTIONS, Ordering::SeqCst);
        assert!(ConnectionGuard::try_acquire().is_none());
        ACTIVE_CLIENT_CONNECTIONS.store(0, Ordering::SeqCst);
        let g = ConnectionGuard::try_acquire();
        assert!(g.is_some());
        assert_eq!(ACTIVE_CLIENT_CONNECTIONS.load(Ordering::SeqCst), 1);
        drop(g);
        assert_eq!(ACTIVE_CLIENT_CONNECTIONS.load(Ordering::SeqCst), 0);
    }
}
