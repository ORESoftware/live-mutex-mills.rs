use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use serde_json::{json, Value};

use live_mutex_mills::client_api::{serve_http, serve_tcp, start_coordinator};
use live_mutex_mills::codec::WireCodec;
use live_mutex_mills::transport::{run_node_with_codec, Command, NodeEvent};
use live_mutex_mills::NodeId;

#[test]
fn external_tcp_and_http_clients_share_exclusive_locks() {
    let cluster = TestCluster::start(3);
    let tcp_addr = serve_tcp("127.0.0.1:0", cluster.clients[0].clone()).unwrap();
    let http_addr = serve_http("127.0.0.1:0", cluster.clients[1].clone()).unwrap();

    let mut tcp = TcpTextClient::connect(tcp_addr);
    let first = tcp.acquire("shared-lock");

    let (http_tx, http_rx) = mpsc::channel();
    thread::spawn(move || {
        let acquired = http_json(http_addr, "POST", "/locks/shared-lock/acquire", json!({}));
        http_tx.send(acquired).unwrap();
    });

    thread::sleep(Duration::from_millis(300));
    assert!(
        http_rx.try_recv().is_err(),
        "HTTP client acquired while TCP client still held the exclusive lock"
    );

    tcp.release(&first.holder);
    let second = http_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("HTTP client should acquire after TCP release");
    assert_eq!(second["ok"], true);
    assert_eq!(second["lock"], "shared-lock");
    assert!(
        second["fence"].as_u64().unwrap() > first.fence,
        "handoff should raise the fence token"
    );
    http_json(
        http_addr,
        "POST",
        "/locks/shared-lock/release",
        json!({"holder": second["holder"].as_str().unwrap()}),
    );

    cluster.shutdown();
}

#[test]
fn external_tcp_and_http_clients_can_use_bounded_semaphores() {
    let cluster = TestCluster::start(3);
    let http_a = serve_http("127.0.0.1:0", cluster.clients[0].clone()).unwrap();
    let tcp_addr = serve_tcp("127.0.0.1:0", cluster.clients[1].clone()).unwrap();
    let http_b = serve_http("127.0.0.1:0", cluster.clients[2].clone()).unwrap();

    let first = http_json(
        http_a,
        "POST",
        "/semaphores/rate-limiter/acquire",
        json!({"limit": 2}),
    );
    assert_eq!(first["ok"], true);

    let mut tcp = TcpTextClient::connect(tcp_addr);
    let second = tcp.semaphore_acquire(2, "rate-limiter");
    assert_ne!(
        first["permit"].as_u64().unwrap(),
        second.permit as u64,
        "two concurrent holders should occupy different semaphore permits"
    );

    let (third_tx, third_rx) = mpsc::channel();
    thread::spawn(move || {
        let acquired = http_json(
            http_b,
            "POST",
            "/semaphores/rate-limiter/acquire",
            json!({"limit": 2}),
        );
        third_tx.send(acquired).unwrap();
    });

    thread::sleep(Duration::from_millis(300));
    assert!(
        third_rx.try_recv().is_err(),
        "third client acquired a capacity-2 semaphore while two holders were active"
    );

    http_json(
        http_a,
        "POST",
        "/semaphores/release",
        json!({"holder": first["holder"].as_str().unwrap()}),
    );
    let third = third_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("third semaphore client should acquire after a permit release");
    assert_eq!(third["ok"], true);
    assert_eq!(third["semaphore"], "rate-limiter");
    assert_eq!(third["limit"], 2);

    tcp.release(&second.holder);
    http_json(
        http_b,
        "POST",
        "/semaphores/release",
        json!({"holder": third["holder"].as_str().unwrap()}),
    );

    cluster.shutdown();
}

#[test]
fn external_clients_can_use_rw_locks() {
    let cluster = TestCluster::start(3);
    let http_a = serve_http("127.0.0.1:0", cluster.clients[0].clone()).unwrap();
    let tcp_addr = serve_tcp("127.0.0.1:0", cluster.clients[1].clone()).unwrap();
    let http_b = serve_http("127.0.0.1:0", cluster.clients[2].clone()).unwrap();

    let first = http_json(
        http_a,
        "POST",
        "/rwlocks/document/read-acquire",
        json!({"limit": 2}),
    );
    assert_eq!(first["ok"], true);
    assert_eq!(first["mode"], "read");

    let mut tcp = TcpTextClient::connect(tcp_addr);
    let second = tcp.rw_acquire("RACQUIRE", 2, "document");
    assert_eq!(second.mode, "read");
    assert_ne!(
        first["permit"].as_u64().unwrap(),
        second.permit.unwrap() as u64,
        "two active readers should occupy different RW reader permits"
    );

    tcp.release(&second.holder);

    let (writer_tx, writer_rx) = mpsc::channel();
    thread::spawn(move || {
        let acquired = http_json(
            http_b,
            "POST",
            "/rwlocks/document/write-acquire",
            json!({"limit": 2}),
        );
        writer_tx.send(acquired).unwrap();
    });

    thread::sleep(Duration::from_millis(300));
    assert!(
        writer_rx.try_recv().is_err(),
        "writer acquired while a reader still held the RW lock"
    );

    let (late_reader_tx, late_reader_rx) = mpsc::channel();
    thread::spawn(move || {
        let acquired = http_json(
            http_a,
            "POST",
            "/rwlocks/document/read-acquire",
            json!({"limit": 2}),
        );
        late_reader_tx.send(acquired).unwrap();
    });

    thread::sleep(Duration::from_millis(300));
    assert!(
        late_reader_rx.try_recv().is_err(),
        "new reader slipped in while a writer was waiting at the RW gate"
    );

    http_json(
        http_a,
        "POST",
        "/rwlocks/release",
        json!({"holder": first["holder"].as_str().unwrap()}),
    );

    let writer = writer_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("writer should acquire after the active reader releases");
    assert_eq!(writer["ok"], true);
    assert_eq!(writer["mode"], "write");

    thread::sleep(Duration::from_millis(300));
    assert!(
        late_reader_rx.try_recv().is_err(),
        "reader acquired while writer held the RW lock"
    );

    http_json(
        http_b,
        "POST",
        "/rwlocks/document/release",
        json!({"holder": writer["holder"].as_str().unwrap()}),
    );
    let late_reader = late_reader_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("reader should acquire after writer release");
    assert_eq!(late_reader["ok"], true);
    assert_eq!(late_reader["mode"], "read");
    http_json(
        http_a,
        "POST",
        "/rwlocks/release",
        json!({"holder": late_reader["holder"].as_str().unwrap()}),
    );

    cluster.shutdown();
}

struct TestCluster {
    cmd_txs: Vec<mpsc::Sender<Command>>,
    clients: Vec<live_mutex_mills::client_api::LockClient>,
}

impl TestCluster {
    fn start(n: usize) -> Self {
        let addrs = loopback_addrs(n);
        let mut cmd_txs = Vec::new();
        let mut clients = Vec::new();

        for id in 0..n as NodeId {
            let (cmd_tx, cmd_rx) = mpsc::channel();
            let (raw_evt_tx, raw_evt_rx) = mpsc::channel::<NodeEvent>();
            let (client, _public_evt_rx) = start_coordinator(cmd_tx.clone(), raw_evt_rx);
            let node_addrs = addrs.clone();
            thread::spawn(move || {
                let _ = run_node_with_codec(id, node_addrs, WireCodec::Text, cmd_rx, raw_evt_tx);
            });
            cmd_txs.push(cmd_tx);
            clients.push(client);
        }

        thread::sleep(Duration::from_millis(900));
        Self { cmd_txs, clients }
    }

    fn shutdown(self) {
        for tx in self.cmd_txs {
            let _ = tx.send(Command::Shutdown);
        }
    }
}

struct TcpTextClient {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

struct TcpAcquire {
    fence: u64,
    holder: String,
}

struct TcpSemaphoreAcquire {
    permit: u32,
    holder: String,
}

struct TcpRwAcquire {
    mode: String,
    permit: Option<u32>,
    holder: String,
}

impl TcpTextClient {
    fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut greeting = String::new();
        reader.read_line(&mut greeting).unwrap();
        Self { stream, reader }
    }

    fn acquire(&mut self, lock: &str) -> TcpAcquire {
        writeln!(self.stream, "ACQUIRE {lock}").unwrap();
        self.stream.flush().unwrap();
        let line = self.read_line();
        assert!(line.starts_with("ACQUIRED "), "{line}");
        let fence = field(&line, "fence=").parse().unwrap();
        let holder = field(&line, "holder=").to_string();
        TcpAcquire { fence, holder }
    }

    fn semaphore_acquire(&mut self, limit: u32, name: &str) -> TcpSemaphoreAcquire {
        writeln!(self.stream, "SEMACQUIRE {limit} {name}").unwrap();
        self.stream.flush().unwrap();
        let line = self.read_line();
        assert!(line.starts_with("SEMACQUIRED "), "{line}");
        let permit = field(&line, "permit=").parse().unwrap();
        let holder = field(&line, "holder=").to_string();
        TcpSemaphoreAcquire { permit, holder }
    }

    fn rw_acquire(&mut self, op: &str, limit: u32, name: &str) -> TcpRwAcquire {
        writeln!(self.stream, "{op} {limit} {name}").unwrap();
        self.stream.flush().unwrap();
        let line = self.read_line();
        assert!(line.starts_with("RWACQUIRED "), "{line}");
        let mode = field(&line, "mode=").to_string();
        let permit = line
            .split_whitespace()
            .find_map(|part| part.strip_prefix("permit="))
            .map(|permit| permit.parse().unwrap());
        let holder = field(&line, "holder=").to_string();
        TcpRwAcquire {
            mode,
            permit,
            holder,
        }
    }

    fn release(&mut self, holder: &str) {
        writeln!(self.stream, "RELEASE {holder}").unwrap();
        self.stream.flush().unwrap();
        let line = self.read_line();
        assert!(
            line.starts_with("RELEASED ")
                || line.starts_with("SEMRELEASED ")
                || line.starts_with("RWRELEASED "),
            "{line}"
        );
    }

    fn read_line(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        line.trim().to_string()
    }
}

fn http_json(addr: SocketAddr, method: &str, path: &str, body: Value) -> Value {
    let (status, value) = http_status_json(addr, method, path, body);
    assert_eq!(status, 200, "expected 200 from {method} {path}, got {status}: {value}");
    value
}

/// Like `http_json` but returns the status line code alongside the parsed body instead of
/// asserting 200 — for exercising error responses (e.g. a rejected semaphore limit).
fn http_status_json(addr: SocketAddr, method: &str, path: &str, body: Value) -> (u16, Value) {
    let body = body.to_string();
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len()
    )
    .unwrap();
    stream.flush().unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status code in response: {response}"));
    (status, serde_json::from_str(body).unwrap())
}

fn field<'a>(line: &'a str, prefix: &str) -> &'a str {
    line.split_whitespace()
        .find_map(|part| part.strip_prefix(prefix))
        .unwrap_or_else(|| panic!("missing {prefix} in {line}"))
}

fn loopback_addrs(n: usize) -> Vec<String> {
    let listeners: Vec<TcpListener> = (0..n)
        .map(|_| TcpListener::bind("127.0.0.1:0").expect("bind ephemeral loopback port"))
        .collect();
    listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().to_string())
        .collect()
}
