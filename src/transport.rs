//! A dependency-free TCP transport that runs a [`Node`] across real sockets.
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
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use crate::codec::{decode, encode};
use crate::{Fence, LockId, Message, Node, NodeId};

/// A command from the application to its local node.
pub enum Command {
    Acquire(LockId),
    Release(LockId),
    Shutdown,
}

/// An event the node reports back to the application.
#[derive(Debug)]
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

const TICK: Duration = Duration::from_millis(500);

/// Run a node until it receives [`Command::Shutdown`]. Blocks the calling
/// thread; spawns helper threads for I/O.
pub fn run_node(
    id: NodeId,
    addrs: Vec<String>,
    cmd_rx: Receiver<Command>,
    evt_tx: Sender<NodeEvent>,
) -> std::io::Result<()> {
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
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let ev_tx = ev_tx.clone();
                thread::spawn(move || read_conn(stream, ev_tx));
            }
        });
    }

    // Dial every peer (retrying) and hand the writable stream to the loop.
    for peer in (0..n as NodeId).filter(|&p| p != id) {
        let addr = addrs[peer as usize].clone();
        let ev_tx = ev_tx.clone();
        thread::spawn(move || loop {
            match TcpStream::connect(&addr) {
                Ok(mut s) => {
                    if writeln!(s, "HELLO {id}").is_ok() && ev_tx.send(Ev::Conn(peer, s)).is_ok() {
                        return;
                    }
                    return;
                }
                Err(_) => thread::sleep(Duration::from_millis(150)),
            }
        });
    }

    // Tick timer.
    {
        let ev_tx = ev_tx.clone();
        thread::spawn(move || loop {
            thread::sleep(TICK);
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
                        write_to(&mut writers, peer, &msg);
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
                write_to(&mut writers, out.to, &out.msg);
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

fn write_to(writers: &mut HashMap<NodeId, TcpStream>, to: NodeId, msg: &Message) {
    if let Some(w) = writers.get_mut(&to) {
        if writeln!(w, "{}", encode(msg)).is_err() {
            // Connection broke; drop it. Leases + the peer's redial recover.
            writers.remove(&to);
        }
    }
}

/// Read a connection: first a `HELLO <id>` handshake, then one message per line.
fn read_conn(stream: TcpStream, ev_tx: Sender<Ev>) {
    let mut r = BufReader::new(stream);
    let mut line = String::new();
    if r.read_line(&mut line).is_err() {
        return;
    }
    let peer: NodeId = match line.trim().strip_prefix("HELLO ").and_then(|s| s.parse().ok()) {
        Some(p) => p,
        None => return,
    };
    loop {
        line.clear();
        match r.read_line(&mut line) {
            Ok(0) | Err(_) => return,
            Ok(_) => {
                if let Some(msg) = decode(line.trim()) {
                    if ev_tx.send(Ev::In(peer, msg)).is_err() {
                        return;
                    }
                }
            }
        }
    }
}
