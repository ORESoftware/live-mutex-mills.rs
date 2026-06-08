use std::io::ErrorKind;
use std::sync::mpsc;
use std::time::Duration;

use live_mutex_mills::codec::WireCodec;
use live_mutex_mills::transport::{
    run_node_with_codec, run_node_with_settings, Command, NodeEvent, TransportSettings,
};

fn run_with(addrs: Vec<&str>, id: u32) -> ErrorKind {
    let (_cmd_tx, cmd_rx) = mpsc::channel::<Command>();
    let (evt_tx, _evt_rx) = mpsc::channel::<NodeEvent>();
    run_node_with_codec(
        id,
        addrs.into_iter().map(String::from).collect(),
        WireCodec::Text,
        cmd_rx,
        evt_tx,
    )
    .expect_err("invalid config should be rejected before the node starts")
    .kind()
}

#[test]
fn rejects_empty_address_list() {
    assert_eq!(run_with(vec![], 0), ErrorKind::InvalidInput);
}

#[test]
fn rejects_out_of_range_node_id() {
    assert_eq!(run_with(vec!["127.0.0.1:0"], 1), ErrorKind::InvalidInput);
}

#[test]
fn rejects_empty_addresses() {
    assert_eq!(run_with(vec![""], 0), ErrorKind::InvalidInput);
}

#[test]
fn rejects_duplicate_addresses() {
    assert_eq!(
        run_with(vec!["127.0.0.1:9100", "127.0.0.1:9100"], 0),
        ErrorKind::InvalidInput
    );
}

#[test]
fn rejects_invalid_transport_settings() {
    let (_cmd_tx, cmd_rx) = mpsc::channel::<Command>();
    let (evt_tx, _evt_rx) = mpsc::channel::<NodeEvent>();
    let settings = TransportSettings {
        tick: Duration::ZERO,
        ..TransportSettings::default()
    };

    assert_eq!(
        run_node_with_settings(0, vec!["127.0.0.1:0".to_string()], settings, cmd_rx, evt_tx,)
            .expect_err("invalid settings should be rejected before bind")
            .kind(),
        ErrorKind::InvalidInput
    );
}
