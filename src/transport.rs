//! A TCP transport that runs a [`Node`] across real sockets.
//!
//! ## Design
//!
//! Each ordered pair of nodes communicates over a **single TCP connection** —
//! the one the *sender* dialed — which gives the FIFO-per-link ordering the
//! protocol requires (see crate docs). A node therefore:
//!
//! - **dials** every peer and uses those outbound connections for *writing*;
//! - **accepts** connections and uses them for *reading*.
//!
//! Everything funnels through one event loop on a single driver thread that
//! owns the `Node`, so the state machine is never touched concurrently. Reader
//! threads, the dialer threads, a tick timer, and the application's command
//! stream all feed [`Ev`]s into that loop.
//!
//! Wall-clock time (milliseconds since start) is passed into the node, driving
//! lease renewal/expiry. The happy path is unaffected by the clock.

use std::collections::HashMap;
use std::collections::HashSet;
use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use crate::codec::WireCodec;
use crate::{Fence, LockId, Message, Node, NodeId, RENEW_INTERVAL};

/// A command from the application to its local node.
pub enum Command {
    Acquire(LockId),
    Release(LockId),
    Shutdown,
}

/// An event the node reports back to the application.
#[derive(Clone, Debug)]
pub enum NodeEvent {
    /// The lock is held; present `fence` to the protected resource.
    Acquired(LockId, Fence),
    /// A held lock was lost (lease lapsed, another holder took over).
    Lost(LockId),
    /// Diagnostic text (connections, etc.).
    Info(String),
}

/// Internal event-loop messages, multiplexed onto one channel.
enum Ev {
    In(NodeId, Message),
    Cmd(Command),
    Conn(NodeId, TcpStream),
    Tick,
}

const DEFAULT_TICK: Duration = Duration::from_millis(500);
const DEFAULT_CONNECT_RETRY: Duration = Duration::from_millis(150);
const DEFAULT_MAX_LINE_FRAME: usize = 1024 * 1024;

/// Runtime settings for the TCP transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransportSettings {
    pub codec: WireCodec,
    pub tick: Duration,
    pub connect_retry: Duration,
    pub max_line_frame: usize,
}

impl Default for TransportSettings {
    fn default() -> Self {
        Self {
            codec: WireCodec::Text,
            tick: DEFAULT_TICK,
            connect_retry: DEFAULT_CONNECT_RETRY,
            max_line_frame: DEFAULT_MAX_LINE_FRAME,
        }
    }
}

impl TransportSettings {
    pub fn with_codec(codec: WireCodec) -> Self {
        Self {
            codec,
            ..Self::default()
        }
    }
}

/// Run a node until it receives [`Command::Shutdown`]. Blocks the calling
/// thread; spawns helper threads for I/O.
pub fn run_node(
    id: NodeId,
    addrs: Vec<String>,
    cmd_rx: Receiver<Command>,
    evt_tx: Sender<NodeEvent>,
) -> std::io::Result<()> {
    run_node_with_codec(id, addrs, WireCodec::Text, cmd_rx, evt_tx)
}

/// Run a node with an explicit wire codec.
pub fn run_node_with_codec(
    id: NodeId,
    addrs: Vec<String>,
    codec: WireCodec,
    cmd_rx: Receiver<Command>,
    evt_tx: Sender<NodeEvent>,
) -> std::io::Result<()> {
    run_node_with_settings(
        id,
        addrs,
        TransportSettings::with_codec(codec),
        cmd_rx,
        evt_tx,
    )
}

/// Run a node with explicit TCP transport settings.
pub fn run_node_with_settings(
    id: NodeId,
    addrs: Vec<String>,
    settings: TransportSettings,
    cmd_rx: Receiver<Command>,
    evt_tx: Sender<NodeEvent>,
) -> std::io::Result<()> {
    validate_config(id, &addrs)?;
    validate_settings(settings)?;
    let n = addrs.len();
    let members: Vec<NodeId> = (0..n as NodeId).collect();
    let mut node = Node::new(id, members);
    let start = Instant::now();
    let now = move || start.elapsed().as_millis() as u64;

    let (ev_tx, ev_rx) = mpsc::channel::<Ev>();

    // Accept inbound (read-only) connections.
    let listener = TcpListener::bind(&addrs[id as usize])?;
    {
        let ev_tx = ev_tx.clone();
        let max_line_frame = settings.max_line_frame;
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let ev_tx = ev_tx.clone();
                thread::spawn(move || read_conn(stream, ev_tx, max_line_frame));
            }
        });
    }

    // Dial every peer (retrying) and hand the writable stream to the loop.
    for peer in (0..n as NodeId).filter(|&p| p != id) {
        let addr = addrs[peer as usize].clone();
        let ev_tx = ev_tx.clone();
        let retry = settings.connect_retry;
        let codec = settings.codec;
        thread::spawn(move || loop {
            match TcpStream::connect(&addr) {
                Ok(mut s) => {
                    if writeln!(s, "HELLO {id} {}", codec.as_str()).is_err() {
                        thread::sleep(retry);
                        continue;
                    }
                    // Keep a read handle so we can notice when the peer closes
                    // this link (e.g. a Kubernetes rolling restart) and re-dial.
                    // The dialed socket is write-only for us, so this reader just
                    // blocks until EOF. Without this the mesh never re-forms
                    // after a peer restarts and consensus wedges.
                    let monitor = match s.try_clone() {
                        Ok(m) => m,
                        Err(_) => {
                            thread::sleep(retry);
                            continue;
                        }
                    };
                    if ev_tx.send(Ev::Conn(peer, s)).is_err() {
                        return; // event loop is gone
                    }
                    let mut r = BufReader::new(monitor);
                    let mut sink = String::new();
                    loop {
                        sink.clear();
                        match r.read_line(&mut sink) {
                            Ok(0) | Err(_) => break, // peer closed; reconnect
                            Ok(_) => {}
                        }
                    }
                }
                Err(_) => thread::sleep(retry),
            }
        });
    }

    // Tick timer.
    {
        let ev_tx = ev_tx.clone();
        let tick = settings.tick;
        thread::spawn(move || loop {
            thread::sleep(tick);
            if ev_tx.send(Ev::Tick).is_err() {
                return;
            }
        });
    }

    // Forward application commands into the loop.
    {
        let ev_tx = ev_tx.clone();
        thread::spawn(move || {
            for cmd in cmd_rx {
                if ev_tx.send(Ev::Cmd(cmd)).is_err() {
                    return;
                }
            }
        });
    }

    // The single-threaded driver: owns the Node, processes one event at a time.
    let mut writers: HashMap<NodeId, TcpStream> = HashMap::new();
    // Messages destined for peers we haven't connected to yet, kept in order.
    let mut pending: HashMap<NodeId, Vec<Message>> = HashMap::new();

    for ev in ev_rx {
        match ev {
            Ev::Conn(peer, stream) => {
                writers.insert(peer, stream);
                if let Some(buffered) = pending.remove(&peer) {
                    for msg in buffered {
                        write_to(&mut writers, peer, settings.codec, &msg);
                    }
                }
                let _ = evt_tx.send(NodeEvent::Info(format!("connected to node {peer}")));
            }
            Ev::In(from, msg) => node.handle(now(), from, msg),
            Ev::Cmd(Command::Acquire(lock)) => node.request(now(), &lock),
            Ev::Cmd(Command::Release(lock)) => node.release(now(), &lock),
            Ev::Cmd(Command::Shutdown) => break,
            Ev::Tick => node.tick(now()),
        }

        for out in node.drain_outbox() {
            if out.to == id {
                // Deliver to self in-process (still FIFO w.r.t. this node).
                let _ = ev_tx.send(Ev::In(id, out.msg));
            } else if writers.contains_key(&out.to) {
                write_to(&mut writers, out.to, settings.codec, &out.msg);
            } else {
                pending.entry(out.to).or_default().push(out.msg);
            }
        }
        for (lock, fence) in node.take_acquired() {
            let _ = evt_tx.send(NodeEvent::Acquired(lock, fence));
        }
        for lock in node.take_lost() {
            let _ = evt_tx.send(NodeEvent::Lost(lock));
        }
    }

    Ok(())
}

fn validate_settings(settings: TransportSettings) -> io::Result<()> {
    if settings.tick.is_zero() {
        return Err(invalid_input("tick interval must be greater than zero"));
    }
    if settings.tick.as_millis() > RENEW_INTERVAL as u128 {
        return Err(invalid_input(
            "tick interval must not exceed renew interval",
        ));
    }
    if settings.connect_retry.is_zero() {
        return Err(invalid_input(
            "connect retry interval must be greater than zero",
        ));
    }
    if settings.max_line_frame == 0 {
        return Err(invalid_input("max line frame must be greater than zero"));
    }
    Ok(())
}

fn validate_config(id: NodeId, addrs: &[String]) -> io::Result<()> {
    if addrs.is_empty() {
        return Err(invalid_input("address list must not be empty"));
    }
    if id as usize >= addrs.len() {
        return Err(invalid_input("node id is outside the address list"));
    }
    if addrs.iter().any(|addr| addr.trim().is_empty()) {
        return Err(invalid_input("addresses must not be empty"));
    }

    let mut seen = HashSet::with_capacity(addrs.len());
    if addrs.iter().any(|addr| !seen.insert(addr)) {
        return Err(invalid_input("addresses must be unique"));
    }

    Ok(())
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn write_to(writers: &mut HashMap<NodeId, TcpStream>, to: NodeId, codec: WireCodec, msg: &Message) {
    if let Some(w) = writers.get_mut(&to) {
        if writeln!(w, "{}", codec.encode_line(msg)).is_err() {
            // Connection broke; drop it. Leases + the peer's redial recover.
            writers.remove(&to);
        }
    }
}

/// Read a connection: first `HELLO <id> [codec]`, then one message per line.
fn read_conn(stream: TcpStream, ev_tx: Sender<Ev>, max_line_frame: usize) {
    let mut r = BufReader::new(stream);
    let mut line = String::new();
    if r.read_line(&mut line).is_err() {
        return;
    }
    if line.len() > max_line_frame {
        return;
    }
    let mut hello = line.split_whitespace();
    if hello.next() != Some("HELLO") {
        return;
    }
    let peer: NodeId = match hello.next().and_then(|s| s.parse().ok()) {
        Some(id) => id,
        None => return,
    };
    let codec = match hello.next() {
        Some(name) => match WireCodec::parse(name) {
            Some(codec) => codec,
            None => return,
        },
        None => WireCodec::Text,
    };
    if hello.next().is_some() {
        return;
    }
    loop {
        line.clear();
        match r.read_line(&mut line) {
            Ok(0) | Err(_) => return,
            Ok(_) => {
                if line.len() > max_line_frame {
                    return;
                }
                if let Some(msg) = codec.decode_line(line.trim()) {
                    if ev_tx.send(Ev::In(peer, msg)).is_err() {
                        return;
                    }
                }
            }
        }
    }
}
