//! Minimal S7 mock server for end-to-end testing of s7trace with
//! turbos7 backend.
//!
//! Implements just enough of the ISO-on-TCP + S7 protocol to let
//! turbos7 complete a connect handshake and read some bytes:
//!   - ISO-on-TCP CR (0xE0) -> CC (0xD0)
//!   - PDU negotiation (function 0xF0) -> echoes 480
//!   - S7 Read (function 0x04) -> response matching turbos7's
//!     parse_read_response layout (see protocol/s7.rs in turbos7)
//!
//! Returned payload pattern: 0xAA, 0xBB, 0xCC, 0xDD, ... (cycled).

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

const ISO_ID: u8 = 0x03;
const ISO_CONN_REQ: u8 = 0xE0;
const ISO_CONN_OK: u8 = 0xD0;
const S7_ID: u8 = 0x32;
const EOT: u8 = 0x80;

fn main() -> std::io::Result<()> {
    let port: u16 = std::env::var("S7MOCK_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(12002);
    let listener = TcpListener::bind(("127.0.0.1", port))?;
    eprintln!("s7mock: listening on 127.0.0.1:{port}");
    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                thread::spawn(move || {
                    if let Err(e) = handle_client(s) {
                        eprintln!("s7mock: client error: {e}");
                    }
                });
            }
            Err(e) => eprintln!("s7mock: accept error: {e}"),
        }
    }
    Ok(())
}

fn handle_client(mut stream: TcpStream) -> std::io::Result<()> {
    stream.set_nodelay(true)?;
    eprintln!("s7mock: client connected from {}", stream.peer_addr()?);
    let mut buf = [0u8; 4096];
    let mut read_count = 0u32;
    loop {
        let n = match stream.read(&mut buf) {
            Ok(0) => return Ok(()),
            Ok(n) => n,
            Err(e) => return Err(e),
        };
        read_count += 1;
        let pkt = &buf[..n];
        if pkt.len() < 7 || pkt[0] != ISO_ID {
            return Ok(());
        }
        eprintln!("s7mock: RX #{} ({} bytes): {}", read_count, pkt.len(), hex(pkt));
        match pkt[5] {
            ISO_CONN_REQ => {
                let mut cc = [0u8; 22];
                let take = pkt.len().min(22);
                cc[..take].copy_from_slice(&pkt[..take]);
                cc[5] = ISO_CONN_OK;
                stream.write_all(&cc[..22])?;
            }
            0xF0 => {
                if let Some(resp) = handle_s7(pkt) {
                    stream.write_all(&resp)?;
                    eprintln!("s7mock: TX ({} bytes): {}", resp.len(), hex(&resp));
                }
            }
            other => {
                eprintln!("s7mock: unknown COTP 0x{other:02X}");
            }
        }
    }
}

/// Build a response for an S7 PDU.
///
/// Layout used (matching turbos7::protocol::s7::parse_read_response):
/// ```
/// [0]    S7_ID = 0x32
/// [1]    ROSCTR = 0x03
/// [2..4] PDU ref (echo)
/// [4..6] <2 padding bytes>
/// [6..8] param_length (big-endian)
/// [8..10] data_length (big-endian)
/// [10..12] error_code (big-endian; 0x0000 = success)
/// [12..12+param_length]  param section
/// [12+param_length..]     data section
///   item header: return_code(1) + transport_size(1) + length(2)
///   payload: read_len bytes
/// ```
fn handle_s7(pkt: &[u8]) -> Option<Vec<u8>> {
    if pkt.len() < 17 || pkt[7] != S7_ID || pkt[8] != 1 {
        return None;
    }
    // PDU negotiation: 25 bytes, function at [17] = 0xF0.
    if pkt.len() == 25 && pkt[17] == 0xF0 {
        let mut resp = vec![0u8; 27];
        resp[0] = ISO_ID; resp[1] = 0x00;
        resp[2] = 0x00; resp[3] = 27;
        resp[4] = 0x02; resp[5] = 0xF0; resp[6] = EOT;
        resp[7] = S7_ID; resp[8] = 0x03;
        resp[9] = 0; resp[10] = 0;
        resp[11] = 0; resp[12] = 8;
        resp[13] = 0; resp[14] = 0;
        resp[15] = 0xF0; resp[16] = 0;
        resp[17] = 0; resp[18] = 1; resp[19] = 0; resp[20] = 1;
        resp[25] = 0x01; resp[26] = 0xE0; // 480
        return Some(resp);
    }
    // Read: function 0x04.
    if pkt[17] != 0x04 || pkt.len() < 31 {
        return None;
    }
    let read_len = ((pkt[23] as usize) << 8) | (pkt[24] as usize);
    let db = ((pkt[25] as u16) << 8) | (pkt[26] as u16);
    let area = pkt[27];
    let address = (pkt[28] as u32) << 16 | (pkt[29] as u32) << 8 | (pkt[30] as u32);
    eprintln!("s7mock: read area=0x{area:02X} db={db} addr={address} len={read_len}");

    let param_len: u16 = 2; // func + items
    let data_len: u16 = 4 + read_len as u16; // item header + payload
    let s7_body = 12 + param_len as usize + data_len as usize;
    let total = 7 + s7_body;
    let mut resp = vec![0u8; total];
    // TPKT
    resp[0] = ISO_ID; resp[1] = 0x00;
    resp[2] = (total >> 8) as u8; resp[3] = (total & 0xFF) as u8;
    // COTP DT
    resp[4] = 0x02; resp[5] = 0xF0; resp[6] = EOT;
    // S7 AckData header
    resp[7] = S7_ID; resp[8] = 0x03;
    resp[9] = pkt[9]; resp[10] = pkt[10]; // echo PDU ref
    resp[11] = 0; resp[12] = 0; // 2 padding bytes
    resp[13] = (param_len >> 8) as u8; resp[14] = (param_len & 0xFF) as u8;
    resp[15] = (data_len >> 8) as u8; resp[16] = (data_len & 0xFF) as u8;
    resp[17] = 0; resp[18] = 0; // error_code = 0
    // Param section
    resp[19] = 0x04; resp[20] = 0x01; // func=Read, items=1
    // Data section: item header
    resp[21] = 0xFF; // item return code (success)
    resp[22] = 0x04; // transport size
    resp[23] = (read_len >> 8) as u8;
    resp[24] = (read_len & 0xFF) as u8;
    // payload
    for i in 0..read_len {
        resp[25 + i] = (0xAA + (i % 4)) as u8;
    }
    Some(resp)
}

fn hex(buf: &[u8]) -> String {
    let mut s = String::with_capacity(buf.len() * 3);
    for b in buf {
        s.push_str(&format!("{:02X} ", b));
    }
    s
}
