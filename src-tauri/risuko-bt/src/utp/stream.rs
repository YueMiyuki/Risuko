//! Per-connection µTP state machine and the [`UtpStream`] `AsyncRead`/`AsyncWrite` handle: each connection is driven by a single background task ([`drive`]) that owns the connection's slice of the shared UDP socket's traffic (fed by the socket router over an mpsc channel) and is the only thing that touches the wire for this connection; the [`UtpStream`] handle shares a [`Mutex<ConnState>`] with the driver (reads drain `recv_ready`, writes append to `send_buf`, a [`Notify`] nudges the driver) and the driver wakes the stream's stored wakers when data arrives or buffer space frees up. Reliability model: in-order byte delivery with a reorder buffer for out-of-order data, cumulative + selective acknowledgements, RFC-6298-style RTO retransmission (Karn's algorithm for RTT sampling), and LEDBAT-lite delay-based congestion control bounded by the peer's advertised window

use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot, Notify};

use risuko_http::{Error as HttpError, ProxyDatagram};

use super::dontfrag::{is_message_too_big, DontFragment};
use super::now_micros;
use super::packet::{PacketType, UtpHeader, HEADER_LEN};
use super::socket::{
    remove_connection_registration, remove_proxy_connection_registration, ConnKey, ConnRegistry,
    ConnectionToken, ProxyConnRegistry,
};
/// Initial IPv4 payload per packet, before path-MTU discovery
const MSS: usize = 1200;
/// uTP header plus the largest SACK extension we send
const UTP_OVERHEAD: usize = HEADER_LEN + 2 + MAX_SACK_BYTES;
/// UDP payload guaranteed by the IPv6 minimum MTU (1280)
const MIN_DATAGRAM_V6: usize = 1232;
/// UDP payload guaranteed by the IPv4 minimum MTU (576)
const MIN_DATAGRAM_V4: usize = 548;
/// Path-MTU search ceilings for a 1500-byte link
const MAX_DATAGRAM_V4: usize = 1472;
const MAX_DATAGRAM_V6: usize = 1452;
/// Stop probing once floor and ceiling are this close
const MTU_SEARCH_GRANULARITY: usize = 16;
/// Raise the ceiling again this often in case the path improved
const MTU_REPROBE_INTERVAL: Duration = Duration::from_secs(600);
const RECV_BUF_MAX: usize = 1024 * 1024;
const SEND_BUF_MAX: usize = 512 * 1024;
/// BEP 29 CCONTROL_TARGET: queuing delay uTP accepts on the uplink
const TARGET_MICROS: f64 = 100_000.0;
/// BEP 29 MAX_CWND_INCREASE_PACKETS_PER_RTT, expressed in bytes as libutp does
const MAX_CWND_INCREASE_BYTES_PER_RTT: f64 = 3000.0;
/// BEP 29 smallest packet size: the window a timeout collapses to
const MIN_WINDOW: usize = 150;
const MAX_CWND: usize = 2 * 1024 * 1024;
const INITIAL_CWND: usize = 3 * MSS;
const MIN_RTO: Duration = Duration::from_millis(500);
const MAX_RTO: Duration = Duration::from_secs(10);
const INITIAL_RTO: Duration = Duration::from_secs(1);
const MAX_RETRANSMITS: u32 = 8;
const SEND_RETRY_DELAY: Duration = Duration::from_millis(100);
const MAX_SEND_RETRIES: u32 = 8;
const LINGER_TIMEOUT: Duration = Duration::from_secs(30);
/// Packets handled per wakeup before acking
const ACK_BATCH: usize = 64;
/// Duplicate acks (or packets SACKed past a hole) that declare a packet lost
const DUP_ACK_THRESHOLD: usize = 3;
/// SACK bitmask cap in bytes (256 packets past the cumulative ack)
const MAX_SACK_BYTES: usize = 32;
/// Packets further than this past `ack_nr` are dropped rather than buffered
const MAX_REORDER_DISTANCE: u16 = 2048;
/// Base delay is the minimum over the last two minutes (BEP 29), tracked in one-minute buckets
const BASE_DELAY_BUCKET: Duration = Duration::from_secs(60);
const BASE_DELAY_BUCKETS: usize = 2;
/// Current delay is the minimum of the last few samples (libutp CUR_DELAY_SIZE)
const CUR_DELAY_SAMPLES: usize = 3;
/// Wait before forcing one packet through a zero receive window
const MIN_ZERO_WINDOW_PROBE: Duration = Duration::from_secs(1);

/// `a` is strictly after `b` in 16-bit sequence space (within half the ring)
fn seq_after(a: u16, b: u16) -> bool {
    let d = a.wrapping_sub(b);
    d != 0 && d < 0x8000
}

/// `a < b` for 32-bit microsecond timestamps that wrap every ~71 minutes
fn wrapping_lt(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) < 0
}

/// Datagram size used before path-MTU discovery proves anything larger
fn initial_datagram(remote: SocketAddr) -> usize {
    if remote.is_ipv6() {
        MIN_DATAGRAM_V6
    } else {
        MSS + UTP_OVERHEAD
    }
}

fn min_datagram(remote: SocketAddr) -> usize {
    if remote.is_ipv6() {
        MIN_DATAGRAM_V6
    } else {
        MIN_DATAGRAM_V4
    }
}

fn max_datagram(remote: SocketAddr) -> usize {
    if remote.is_ipv6() {
        MAX_DATAGRAM_V6
    } else {
        MAX_DATAGRAM_V4
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    SynSent,
    Connected,
    FinSent,
    Closed,
}

/// A packet we've transmitted that is awaiting acknowledgement; stored un-encoded so retransmissions carry fresh timestamps / ack_nr / window
struct OutPacket {
    packet_type: PacketType,
    seq_nr: u16,
    payload: Vec<u8>,
    sent_at: Instant,
    transmissions: u32,
    /// Lost to a timeout and waiting for window room; not counted as in flight
    need_resend: bool,
    /// Already fast-retransmitted for this loss episode
    fast_resent: bool,
}

/// LEDBAT one-way delay tracking
#[derive(Default)]
struct DelayHistory {
    /// Minimum of each completed bucket, oldest first
    history: VecDeque<u32>,
    bucket_min: Option<u32>,
    bucket_start: Option<Instant>,
    recent: VecDeque<u32>,
}

impl DelayHistory {
    /// Record a delay sample and return the queuing delay (`our_delay`) in microseconds
    fn add_sample(&mut self, sample: u32, now: Instant) -> u32 {
        match (self.bucket_start, self.bucket_min) {
            (Some(start), Some(min)) if now.duration_since(start) >= BASE_DELAY_BUCKET => {
                self.history.push_back(min);
                while self.history.len() > BASE_DELAY_BUCKETS {
                    self.history.pop_front();
                }
                self.bucket_start = Some(now);
                self.bucket_min = Some(sample);
            }
            (Some(_), Some(min)) => {
                if wrapping_lt(sample, min) {
                    self.bucket_min = Some(sample);
                }
            }
            _ => {
                self.bucket_start = Some(now);
                self.bucket_min = Some(sample);
            }
        }
        self.recent.push_back(sample);
        while self.recent.len() > CUR_DELAY_SAMPLES {
            self.recent.pop_front();
        }
        let min_of = |values: &mut dyn Iterator<Item = u32>| {
            values.reduce(|a, b| if wrapping_lt(b, a) { b } else { a })
        };
        let base =
            min_of(&mut self.history.iter().copied().chain(self.bucket_min)).unwrap_or(sample);
        let current = min_of(&mut self.recent.iter().copied()).unwrap_or(sample);
        if wrapping_lt(current, base) {
            0
        } else {
            current.wrapping_sub(base)
        }
    }
}

pub(crate) struct ConnState {
    state: State,
    remote: SocketAddr,
    conn_id_send: u16,
    is_initiator: bool,
    /// Payload bytes per normal packet, derived from `mtu_floor`
    packet_size: usize,
    /// Path-MTU discovery bounds in UDP payload bytes: the floor is proven, the ceiling not yet ruled out
    pmtud: bool,
    mtu_floor: usize,
    mtu_ceiling: usize,
    /// Outstanding probe: sequence number and datagram size
    mtu_probe: Option<(u16, usize)>,
    /// Encoded probe waiting to be sent with don't-fragment set
    probe_out: Option<(u16, Vec<u8>)>,
    mtu_reprobe_at: Instant,

    seq_nr: u16,
    ack_nr: u16,

    send_buf: VecDeque<u8>,
    unacked: VecDeque<OutPacket>,

    recv_ready: VecDeque<u8>,
    reorder: BTreeMap<u16, Vec<u8>>,

    peer_wnd: u32,
    max_window: usize,
    slow_start: bool,
    delay: DelayHistory,

    rtt: f64,
    rtt_var: f64,
    rto: Duration,
    /// Retransmission timer start, restarted whenever an ack makes progress
    rto_base: Option<Instant>,
    reply_micros: u32,

    /// Last cumulative ack received and how many pure acks repeated it
    last_ack: Option<u16>,
    dup_acks: usize,
    /// Zero-window probe timer and the permission it grants once fired
    probe_at: Option<Instant>,
    probe_due: bool,
    /// Last advertised receive window, to know when a window update is due
    last_advertised: std::cell::Cell<u32>,

    needs_ack: bool,
    want_fin: bool,
    peer_fin: Option<u16>,
    eof: bool,
    error: Option<io::ErrorKind>,

    recovery_seq: Option<u16>,

    outbox: Vec<Vec<u8>>,
    send_retry_at: Option<Instant>,
    send_retry_count: u32,
    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
    connect_notify: Option<oneshot::Sender<io::Result<()>>>,
}

impl ConnState {
    fn advertised_window(&self) -> u32 {
        // Out-of-order bytes in `reorder` also consume receive buffer, so count them in the advertised window
        let reorder_bytes: usize = self.reorder.values().map(|b| b.len()).sum();
        let used = self.recv_ready.len().saturating_add(reorder_bytes);
        RECV_BUF_MAX.saturating_sub(used) as u32
    }

    fn header(&self, packet_type: PacketType, connection_id: u16, seq_nr: u16) -> UtpHeader {
        let wnd_size = self.advertised_window();
        self.last_advertised.set(wnd_size);
        UtpHeader {
            packet_type,
            connection_id,
            timestamp_micros: now_micros(),
            timestamp_diff_micros: self.reply_micros,
            wnd_size,
            seq_nr,
            ack_nr: self.ack_nr,
            selective_ack: self.build_selective_ack(),
        }
    }

    /// Encode an outstanding packet with current ack/window/timestamps
    fn encode(&self, p: &OutPacket) -> Vec<u8> {
        // The SYN is special-cased to carry our *receive* id (send-1); every other packet carries our send id
        let conn_id = if p.packet_type == PacketType::Syn {
            self.conn_id_send.wrapping_sub(1)
        } else {
            self.conn_id_send
        };
        self.header(p.packet_type, conn_id, p.seq_nr)
            .encode(&p.payload)
    }

    /// Standalone ST_STATE ack; carries the next seq without consuming it
    fn encode_state(&self) -> Vec<u8> {
        self.header(PacketType::State, self.conn_id_send, self.seq_nr)
            .encode(&[])
    }

    /// SACK bitmask for buffered out-of-order packets; bit `i` (LSB-first) acks `ack_nr + 2 + i`
    fn build_selective_ack(&self) -> Option<Vec<u8>> {
        let base = self.ack_nr.wrapping_add(2);
        let max_bits = MAX_SACK_BYTES * 8;
        let highest = self
            .reorder
            .keys()
            .map(|&seq| seq.wrapping_sub(base) as usize)
            .filter(|&bit| bit < max_bits)
            .max()?;
        let mut mask = vec![0u8; (highest / 32 + 1) * 4];
        for &seq in self.reorder.keys() {
            let bit = seq.wrapping_sub(base) as usize;
            if bit < mask.len() * 8 {
                mask[bit / 8] |= 1 << (bit % 8);
            }
        }
        Some(mask)
    }

    fn bytes_in_flight(&self) -> usize {
        self.unacked
            .iter()
            .filter(|p| !p.need_resend)
            .map(|p| p.payload.len())
            .sum()
    }

    /// Whether `len` bytes fit both windows; an idle connection may exceed a tiny congestion window by one packet (BEP 29), but a zero receive window waits for a probe
    fn may_send(&self, len: usize) -> bool {
        let in_flight = self.bytes_in_flight();
        let fits_peer = in_flight + len <= self.peer_wnd as usize;
        if in_flight == 0 {
            return fits_peer || self.probe_due;
        }
        fits_peer && in_flight + len <= self.max_window
    }

    /// Resend timed-out packets, then packetize `send_buf`, while the windows allow
    fn fill_send_window(&mut self) {
        if self.state == State::SynSent || self.state == State::Closed {
            return;
        }
        while let Some(idx) = self.unacked.iter().position(|p| p.need_resend) {
            if !self.may_send(self.unacked[idx].payload.len()) {
                break;
            }
            self.retransmit(idx);
        }
        if self.state == State::Connected {
            while !self.send_buf.is_empty() {
                if self.should_probe() {
                    let payload = self.probe_payload();
                    if self.send_buf.len() >= payload && self.may_send(payload) {
                        let payload: Vec<u8> = self.send_buf.drain(..payload).collect();
                        self.transmit_probe(payload);
                        continue;
                    }
                }
                let take = self.send_buf.len().min(self.packet_size);
                // Nagle: hold a partial packet while data is in flight
                if take < self.packet_size && self.bytes_in_flight() > 0 && !self.want_fin {
                    break;
                }
                if !self.may_send(take) {
                    break;
                }
                let payload: Vec<u8> = self.send_buf.drain(..take).collect();
                self.transmit_new(PacketType::Data, payload);
            }
        }
        // Nothing in flight will reopen a closed receive window, so schedule a probe
        let blocked = !self.send_buf.is_empty() || self.unacked.iter().any(|p| p.need_resend);
        if blocked && self.bytes_in_flight() == 0 && self.probe_at.is_none() && !self.probe_due {
            self.probe_at = Some(Instant::now() + self.rto.max(MIN_ZERO_WINDOW_PROBE));
        }
    }

    /// Start the retransmission timer if it isn't running
    fn arm_rto(&mut self) {
        if self.rto_base.is_none() {
            self.rto_base = Some(Instant::now());
        }
    }

    fn set_mtu_floor(&mut self, floor: usize) {
        self.mtu_floor = floor;
        self.packet_size = floor - UTP_OVERHEAD;
    }

    /// Settle after a probe resolves; a floor above the ceiling means the path shrank, so search again from halfway down
    fn update_mtu_limits(&mut self) {
        if self.mtu_floor > self.mtu_ceiling {
            self.mtu_ceiling = self.mtu_floor;
            let floor = (min_datagram(self.remote) + self.mtu_ceiling) / 2;
            self.set_mtu_floor(floor);
        }
        self.mtu_probe = None;
    }

    /// Probe while the search is open and the window is wide enough to surround the probe with normal packets (libtorrent's rule)
    fn should_probe(&self) -> bool {
        self.pmtud
            && self.state == State::Connected
            && self.mtu_probe.is_none()
            && self.probe_out.is_none()
            && self.mtu_ceiling >= self.mtu_floor + MTU_SEARCH_GRANULARITY
            && self.max_window > 3 * self.mtu_floor
    }

    /// Probe payload: the midpoint datagram less worst-case overhead
    fn probe_payload(&self) -> usize {
        (self.mtu_floor + self.mtu_ceiling) / 2 - UTP_OVERHEAD
    }

    /// Like [`Self::transmit_new`] for a probe, which is sent with don't-fragment set
    fn transmit_probe(&mut self, payload: Vec<u8>) {
        let p = OutPacket {
            packet_type: PacketType::Data,
            seq_nr: self.seq_nr,
            payload,
            sent_at: Instant::now(),
            transmissions: 1,
            need_resend: false,
            fast_resent: false,
        };
        self.seq_nr = self.seq_nr.wrapping_add(1);
        let bytes = self.encode(&p);
        self.mtu_probe = Some((p.seq_nr, bytes.len()));
        self.probe_out = Some((p.seq_nr, bytes));
        self.unacked.push_back(p);
        self.probe_due = false;
        self.arm_rto();
        self.needs_ack = false;
    }

    /// The probe arrived: its size works
    fn on_mtu_probe_acked(&mut self, seq: u16) {
        if let Some((probe, size)) = self.mtu_probe {
            if probe == seq {
                self.set_mtu_floor(self.mtu_floor.max(size));
                self.update_mtu_limits();
            }
        }
    }

    /// If `seq` is the probe, lower the ceiling and return true so its loss isn't treated as congestion
    fn on_mtu_probe_lost(&mut self, seq: u16) -> bool {
        match self.mtu_probe {
            Some((probe, size)) if probe == seq => {
                self.mtu_ceiling = size - 1;
                self.update_mtu_limits();
                true
            }
            _ => false,
        }
    }

    /// The OS refused the probe (e.g. EMSGSIZE): lower the ceiling and resend without don't-fragment
    pub(crate) fn mtu_probe_send_failed(&mut self, seq: u16) {
        self.on_mtu_probe_lost(seq);
        if let Some(idx) = self.unacked.iter().position(|p| p.seq_nr == seq) {
            self.retransmit(idx);
        }
    }

    /// Assign a fresh sequence number to a DATA/FIN packet and queue it
    fn transmit_new(&mut self, packet_type: PacketType, payload: Vec<u8>) {
        let p = OutPacket {
            packet_type,
            seq_nr: self.seq_nr,
            payload,
            sent_at: Instant::now(),
            transmissions: 1,
            need_resend: false,
            fast_resent: false,
        };
        self.seq_nr = self.seq_nr.wrapping_add(1);
        self.outbox.push(self.encode(&p));
        self.unacked.push_back(p);
        self.probe_due = false;
        self.arm_rto();
        // A DATA/FIN packet carries our ack_nr, so it doubles as an ack
        self.needs_ack = false;
    }

    /// Re-send the unacked packet at `idx` with fresh header fields
    fn retransmit(&mut self, idx: usize) {
        let Some(p) = self.unacked.get_mut(idx) else {
            return;
        };
        p.sent_at = Instant::now();
        p.transmissions += 1;
        p.need_resend = false;
        let bytes = self.encode(&self.unacked[idx]);
        self.outbox.push(bytes);
        self.probe_due = false;
        self.arm_rto();
        self.needs_ack = false;
    }

    /// Halve the congestion window once per loss episode
    fn on_loss(&mut self) {
        if self.recovery_seq.is_none() {
            self.recovery_seq = Some(self.seq_nr.wrapping_sub(1));
            self.max_window = (self.max_window / 2).max(MIN_WINDOW);
            self.slow_start = false;
        }
    }

    /// Apply a cumulative ack and count duplicate acks; returns acked payload bytes
    fn process_ack(&mut self, header: &UtpHeader) -> usize {
        let ack_nr = header.ack_nr;
        let mut acked_bytes = 0usize;
        let mut acked_any = false;
        while let Some(front) = self.unacked.front() {
            if seq_after(front.seq_nr, ack_nr) {
                break; // front is beyond the ack point
            }
            let p = self.unacked.pop_front().unwrap();
            acked_any = true;
            acked_bytes += p.payload.len();
            // Karn: only sample RTT from packets sent exactly once
            if p.transmissions == 1 {
                self.update_rtt(p.sent_at.elapsed());
                self.on_mtu_probe_acked(p.seq_nr);
            }
        }
        if acked_any {
            self.dup_acks = 0;
            // Recovery ends once its marker is cumulatively acked, re-arming decrease for the next loss
            if let Some(rseq) = self.recovery_seq {
                if !seq_after(rseq, ack_nr) {
                    self.recovery_seq = None;
                }
            }
            self.rto_base = (!self.unacked.is_empty()).then(Instant::now);
        } else if header.packet_type == PacketType::State
            && self.last_ack == Some(ack_nr)
            && !self.unacked.is_empty()
        {
            self.dup_acks += 1;
            if self.dup_acks == DUP_ACK_THRESHOLD {
                self.fast_retransmit_front(ack_nr);
            }
        }
        self.last_ack = Some(ack_nr);
        acked_bytes
    }

    /// Three duplicate acks: `ack_nr + 1` is presumed lost
    fn fast_retransmit_front(&mut self, ack_nr: u16) {
        let lost = ack_nr.wrapping_add(1);
        if let Some(idx) = self
            .unacked
            .iter()
            .position(|p| p.seq_nr == lost && !p.fast_resent)
        {
            self.unacked[idx].fast_resent = true;
            if !self.on_mtu_probe_lost(lost) {
                self.on_loss();
            }
            self.retransmit(idx);
        }
    }

    /// Apply a selective ack, fast-retransmitting packets with three or more SACKed packets after them; returns acked payload bytes
    fn process_selective_ack(&mut self, ack_nr: u16, mask: &[u8]) -> usize {
        let base = ack_nr.wrapping_add(2);
        let mut sacked: Vec<u16> = Vec::new();
        for (byte_idx, byte) in mask.iter().enumerate() {
            for bit in 0..8 {
                if byte & (1 << bit) != 0 {
                    sacked.push(base.wrapping_add((byte_idx * 8 + bit) as u16));
                }
            }
        }
        if sacked.is_empty() {
            return 0;
        }
        let mut acked_bytes = 0usize;
        let mut samples = Vec::new();
        self.unacked.retain(|p| {
            if sacked.contains(&p.seq_nr) {
                acked_bytes += p.payload.len();
                if p.transmissions == 1 {
                    samples.push((p.seq_nr, p.sent_at.elapsed()));
                }
                false
            } else {
                true
            }
        });
        for (seq, sample) in samples {
            self.update_rtt(sample);
            self.on_mtu_probe_acked(seq);
        }
        let lost: Vec<usize> = self
            .unacked
            .iter()
            .enumerate()
            .filter(|(_, p)| {
                !p.fast_resent
                    && sacked.iter().filter(|&&s| seq_after(s, p.seq_nr)).count()
                        >= DUP_ACK_THRESHOLD
            })
            .map(|(idx, _)| idx)
            .collect();
        if !lost.is_empty() {
            // A lost probe says "too big", not "congested"
            let mut congestion = false;
            for &idx in &lost {
                let seq = self.unacked[idx].seq_nr;
                congestion |= !self.on_mtu_probe_lost(seq);
            }
            if congestion {
                self.on_loss();
            }
            for idx in lost {
                self.unacked[idx].fast_resent = true;
                self.retransmit(idx);
            }
        }
        acked_bytes
    }

    fn update_rtt(&mut self, sample: Duration) {
        let s = sample.as_secs_f64();
        if self.rtt == 0.0 {
            self.rtt = s;
            self.rtt_var = s / 2.0;
        } else {
            self.rtt_var = 0.75 * self.rtt_var + 0.25 * (self.rtt - s).abs();
            self.rtt = 0.875 * self.rtt + 0.125 * s;
        }
        let rto = Duration::from_secs_f64(self.rtt + 4.0 * self.rtt_var);
        self.rto = rto.clamp(MIN_RTO, MAX_RTO);
    }

    /// BEP 29 LEDBAT window update, grown only while the window is the limit; slow start runs until the delay target or a loss
    fn update_cwnd(&mut self, their_delay: u32, acked_bytes: usize, in_flight_before: usize) {
        if acked_bytes == 0 {
            return;
        }
        // A zero difference means the peer has no delay sample yet
        let delay_factor = if their_delay == 0 {
            0.0
        } else {
            let our_delay = self.delay.add_sample(their_delay, Instant::now()) as f64;
            (TARGET_MICROS - our_delay) / TARGET_MICROS
        };
        let window = self.max_window.max(1) as f64;
        let window_factor = (acked_bytes as f64).min(window) / window.max(acked_bytes as f64);
        let mut gain = MAX_CWND_INCREASE_BYTES_PER_RTT * delay_factor * window_factor;
        let window_limited = in_flight_before + self.packet_size > self.max_window;
        if gain > 0.0 && !window_limited {
            gain = 0.0;
        }
        if self.slow_start {
            if delay_factor < 0.0 {
                self.slow_start = false;
            } else if window_limited {
                gain = gain.max(acked_bytes as f64);
            }
        }
        let next = self.max_window as f64 + gain;
        self.max_window = (next as i64).clamp(MIN_WINDOW as i64, MAX_CWND as i64) as usize;
    }

    /// Process one incoming packet
    fn handle_packet(&mut self, header: &UtpHeader, payload: &[u8]) {
        if self.state == State::Closed {
            return;
        }
        if header.packet_type == PacketType::Syn {
            // A retransmitted SYN means our handshake ack was lost; answer it again
            if !self.is_initiator {
                self.needs_ack = true;
            }
            return;
        }
        // An ack for a packet we never sent is forged or from a stale connection
        if seq_after(header.ack_nr, self.seq_nr.wrapping_sub(1)) {
            return;
        }
        let previous_peer_wnd = self.peer_wnd;
        self.peer_wnd = header.wnd_size;
        if self.peer_wnd > previous_peer_wnd {
            self.probe_at = None;
        }
        // Measure the one-way delay of *this* packet so we can echo it back
        self.reply_micros = now_micros().wrapping_sub(header.timestamp_micros);

        // Handshake completion: first STATE after our SYN
        if self.state == State::SynSent && header.packet_type == PacketType::State {
            self.state = State::Connected;
            // Peer's STATE carries its next-data seq; we've received nothing yet
            self.ack_nr = header.seq_nr.wrapping_sub(1);
            if let Some(tx) = self.connect_notify.take() {
                let _ = tx.send(Ok(()));
            }
            self.notify_write();
        }

        let in_flight_before = self.bytes_in_flight();
        let unacked_before = self.unacked.len();
        let mut acked = self.process_ack(header);
        if let Some(mask) = &header.selective_ack {
            acked += self.process_selective_ack(header.ack_nr, mask);
        }
        self.update_cwnd(header.timestamp_diff_micros, acked, in_flight_before);
        // Wake on any acked packet: an acked FIN carries no payload but completes `shutdown`
        if self.unacked.len() < unacked_before {
            self.notify_write();
        }

        match header.packet_type {
            PacketType::Reset => {
                self.fail(io::ErrorKind::ConnectionReset);
                return;
            }
            PacketType::Data | PacketType::Fin => {
                self.accept_inorder(header, payload);
            }
            PacketType::State | PacketType::Syn => {}
        }

        // After a FIN whose sequence we've now reached in order, signal EOF
        if let Some(fin) = self.peer_fin {
            if !seq_after(fin, self.ack_nr) {
                self.eof = true;
                self.notify_read();
            }
        }
        self.maybe_finish();
    }

    /// Place a DATA/FIN payload in order, buffering out-of-order arrivals
    fn accept_inorder(&mut self, header: &UtpHeader, payload: &[u8]) {
        // Ack every DATA/FIN, even when dropped
        self.needs_ack = true;
        let distance = header.seq_nr.wrapping_sub(self.ack_nr);
        if distance == 0 || distance >= 0x8000 {
            return; // duplicate / already acked
        }
        if distance > MAX_REORDER_DISTANCE {
            return; // far outside any window we advertised
        }
        // A sender ignoring our zero window: drop rather than buffer without bound
        if self.advertised_window() == 0 && !payload.is_empty() {
            return;
        }
        if distance == 1 {
            self.consume(header.packet_type, header.seq_nr, payload);
            // Drain any contiguous reorder-buffer entries
            loop {
                let next = self.ack_nr.wrapping_add(1);
                let Some(buf) = self.reorder.remove(&next) else {
                    break;
                };
                // A buffered FIN is recorded; its (empty) payload adds nothing
                let ty = if Some(next) == self.peer_fin {
                    PacketType::Fin
                } else {
                    PacketType::Data
                };
                self.consume(ty, next, &buf);
            }
        } else {
            // Record an out-of-order FIN regardless of buffer room: it carries no payload, so it costs nothing, and dropping it could stall EOF until a retransmission refills the reorder buffer
            if header.packet_type == PacketType::Fin {
                self.peer_fin = Some(header.seq_nr);
            }
            if self.reorder.len() < RECV_BUF_MAX / self.packet_size {
                self.reorder.insert(header.seq_nr, payload.to_vec());
            }
        }
    }

    /// Advance `ack_nr` past `seq`, delivering DATA bytes to the reader and recording a FIN
    fn consume(&mut self, ty: PacketType, seq: u16, payload: &[u8]) {
        self.ack_nr = seq;
        if ty == PacketType::Fin {
            self.peer_fin = Some(seq);
        } else if !payload.is_empty() {
            self.recv_ready.extend(payload.iter().copied());
            self.notify_read();
        }
    }

    /// Fire due timers: the BEP 29 retransmission timeout and the zero-window probe
    fn check_timers(&mut self) {
        let now = Instant::now();
        if let Some(at) = self.probe_at {
            if now >= at {
                self.probe_at = None;
                self.probe_due = true;
            }
        }
        if self.pmtud && now >= self.mtu_reprobe_at {
            self.mtu_reprobe_at = now + MTU_REPROBE_INTERVAL;
            self.mtu_ceiling = self.mtu_ceiling.max(max_datagram(self.remote));
        }
        let Some(base) = self.rto_base else {
            return;
        };
        if self.unacked.is_empty() {
            self.rto_base = None;
            return;
        }
        if now < base + self.rto {
            return;
        }
        if self
            .unacked
            .front()
            .is_some_and(|p| p.transmissions >= MAX_RETRANSMITS)
        {
            self.fail(io::ErrorKind::TimedOut);
            return;
        }
        // A lone timed-out probe was too big, which isn't congestion
        if self.unacked.len() == 1 && self.on_mtu_probe_lost(self.unacked[0].seq_nr) {
            self.rto_base = None;
            self.retransmit(0);
            return;
        }
        // Everything is resent without don't-fragment, so the probe can't prove anything
        self.mtu_probe = None;
        // Timeout: collapse the window, back off, and resend everything as the window reopens
        self.max_window = MIN_WINDOW;
        self.slow_start = false;
        self.rto = (self.rto * 2).min(MAX_RTO);
        self.dup_acks = 0;
        self.recovery_seq = None;
        for p in self.unacked.iter_mut() {
            p.need_resend = true;
            p.fast_resent = false;
        }
        self.rto_base = None;
        self.retransmit(0);
    }

    /// Emit a FIN once the send buffer has drained, then mark FinSent
    fn maybe_send_fin(&mut self) {
        if self.want_fin && self.state == State::Connected && self.send_buf.is_empty() {
            self.transmit_new(PacketType::Fin, Vec::new());
            self.state = State::FinSent;
        }
    }

    /// Transition a half-closed connection to fully closed once our FIN is acked and we've seen the peer's FIN, so the driver can wind down
    fn maybe_finish(&mut self) {
        if self.state == State::FinSent && self.unacked.is_empty() && self.eof {
            self.state = State::Closed;
        }
    }

    /// Send a window update once a reader drains a nearly full receive buffer
    fn note_window_update(&mut self) {
        let threshold = self.packet_size as u32;
        if self.last_advertised.get() < threshold && self.advertised_window() >= threshold {
            self.needs_ack = true;
        }
    }

    /// Next instant the driver must wake to do timer work, if any
    fn next_deadline(&self) -> Option<Instant> {
        let retransmit_at = self
            .rto_base
            .filter(|_| !self.unacked.is_empty())
            .map(|base| base + self.rto);
        [retransmit_at, self.probe_at, self.send_retry_at]
            .into_iter()
            .flatten()
            .min()
    }

    fn schedule_send_retry(&mut self) -> bool {
        if self.send_retry_count >= MAX_SEND_RETRIES {
            return false;
        }
        self.send_retry_count += 1;
        self.send_retry_at = Some(Instant::now() + SEND_RETRY_DELAY);
        true
    }

    fn retry_failed_datagrams(&mut self, mut datagrams: Vec<Vec<u8>>) -> bool {
        if !self.schedule_send_retry() {
            return false;
        }
        datagrams.append(&mut self.outbox);
        self.outbox = datagrams;
        true
    }

    fn record_send_success(&mut self) {
        self.send_retry_count = 0;
    }

    fn fail(&mut self, kind: io::ErrorKind) {
        if self.error.is_none() {
            self.error = Some(kind);
        }
        self.state = State::Closed;
        self.eof = true;
        self.unacked.clear();
        self.send_buf.clear();
        self.rto_base = None;
        self.probe_at = None;
        if let Some(tx) = self.connect_notify.take() {
            let _ = tx.send(Err(io::Error::from(kind)));
        }
        self.notify_read();
        self.notify_write();
    }

    /// Seed responder state from the initiating SYN: it consumed `syn.seq_nr`, so our first expected DATA is the next sequence number
    pub(crate) fn seed_responder(&mut self, syn: &UtpHeader) {
        self.ack_nr = syn.seq_nr;
        self.peer_wnd = syn.wnd_size;
        self.reply_micros = now_micros().wrapping_sub(syn.timestamp_micros);
    }

    /// Abandon the connection immediately (e.g. on connect timeout) so the driver exits promptly instead of retransmitting to a dead peer
    pub(crate) fn force_close(&mut self) {
        self.state = State::Closed;
        self.error.get_or_insert(io::ErrorKind::TimedOut);
        self.unacked.clear();
        self.send_buf.clear();
        self.rto_base = None;
        self.probe_at = None;
    }

    fn notify_read(&mut self) {
        if let Some(w) = self.read_waker.take() {
            w.wake();
        }
    }

    fn notify_write(&mut self) {
        if let Some(w) = self.write_waker.take() {
            w.wake();
        }
    }
}

/// Shared between the [`UtpStream`] handle and its driver task
pub(crate) struct Shared {
    pub(crate) state: Mutex<ConnState>,
    /// Nudges the driver after the app writes / requests shutdown
    pub(crate) nudge: Notify,
}

/// Whether a freshly-created connection initiates (sends a SYN) or responds. Carries the establishment notifier for the initiator; consumed by [`drive`]
pub(crate) enum Role {
    /// Outgoing dial; the driver sends a SYN and reports establishment here
    Initiator(oneshot::Sender<io::Result<()>>),
    /// Inbound connection accepted from a peer's SYN; already Connected
    Responder,
}

/// `Copy` view of [`Role`] used to pick a connection's initial state without consuming the (non-`Clone`) establishment notifier
#[derive(Clone, Copy)]
pub(crate) enum RoleKind {
    Initiator,
    Responder,
}

/// Configuration handed to a connection driver by the socket layer
pub(crate) struct DriverConfig {
    pub transport: DatagramTransport,
    pub remote: SocketAddr,
    pub incoming: mpsc::UnboundedReceiver<(UtpHeader, Bytes)>,
    pub registry: ConnRegistry,
    pub key: ConnKey,
    pub token: ConnectionToken,
    pub proxy_registry: Option<ProxyConnRegistry>,
}

#[derive(Clone)]
pub(crate) enum DatagramTransport {
    /// Shared UDP socket, with don't-fragment control where supported
    Direct(Arc<UdpSocket>, Option<Arc<DontFragment>>),
    Proxy(Arc<ProxyDatagram>),
}

/// Create the shared state for a new connection
pub(crate) fn new_shared(remote: SocketAddr, conn_id_send: u16, kind: RoleKind) -> Arc<Shared> {
    let state = match kind {
        RoleKind::Initiator => State::SynSent,
        RoleKind::Responder => State::Connected,
    };
    Arc::new(Shared {
        state: Mutex::new(ConnState {
            state,
            remote,
            conn_id_send,
            is_initiator: matches!(kind, RoleKind::Initiator),
            packet_size: initial_datagram(remote) - UTP_OVERHEAD,
            pmtud: false,
            mtu_floor: initial_datagram(remote),
            mtu_ceiling: max_datagram(remote),
            mtu_probe: None,
            probe_out: None,
            mtu_reprobe_at: Instant::now() + MTU_REPROBE_INTERVAL,
            // Initiator's SYN consumes seq 1, so the next DATA is seq 2. Responder picks a random initial sequence
            seq_nr: match kind {
                RoleKind::Initiator => 2,
                RoleKind::Responder => rand::random::<u16>() | 1,
            },
            ack_nr: 0,
            send_buf: VecDeque::new(),
            unacked: VecDeque::new(),
            recv_ready: VecDeque::new(),
            reorder: BTreeMap::new(),
            peer_wnd: RECV_BUF_MAX as u32,
            max_window: INITIAL_CWND,
            slow_start: true,
            delay: DelayHistory::default(),
            rtt: 0.0,
            rtt_var: 0.0,
            rto: INITIAL_RTO,
            rto_base: None,
            reply_micros: 0,
            last_ack: None,
            dup_acks: 0,
            probe_at: None,
            probe_due: false,
            last_advertised: std::cell::Cell::new(RECV_BUF_MAX as u32),
            needs_ack: false,
            want_fin: false,
            peer_fin: None,
            eof: false,
            error: None,
            recovery_seq: None,
            outbox: Vec::new(),
            send_retry_at: None,
            send_retry_count: 0,
            read_waker: None,
            write_waker: None,
            connect_notify: None,
        }),
        nudge: Notify::new(),
    })
}

/// The single task that owns a connection's wire traffic for its lifetime
pub(crate) async fn drive(shared: Arc<Shared>, mut cfg: DriverConfig, role: Role) {
    // Kick off the handshake / initial ack and arm the connect notifier
    {
        let mut st = shared.state.lock();
        st.pmtud = matches!(cfg.transport, DatagramTransport::Direct(_, Some(_)));
        if let Role::Initiator(tx) = role {
            st.connect_notify = Some(tx);
            // Send the SYN (seq 1). It lives in `unacked` for retransmission
            let syn = OutPacket {
                packet_type: PacketType::Syn,
                seq_nr: 1,
                payload: Vec::new(),
                sent_at: Instant::now(),
                transmissions: 1,
                need_resend: false,
                fast_resent: false,
            };
            let syn_bytes = st.encode(&syn);
            st.outbox.push(syn_bytes);
            st.unacked.push_back(syn);
            st.arm_rto();
        } else {
            // Responder: ack_nr was set by the socket from the SYN; send STATE
            let state_bytes = st.encode_state();
            st.outbox.push(state_bytes);
        }
    }
    flush(&shared, &cfg).await;

    let mut closed_since: Option<Instant> = None;
    loop {
        let deadline = {
            let st = shared.state.lock();
            if st.state == State::Closed && closed_since.is_none() {
                closed_since = Some(Instant::now());
            }
            st.next_deadline()
        };

        // Stop lingering once closed and drained
        if let Some(since) = closed_since {
            let drained = {
                let st = shared.state.lock();
                st.unacked.is_empty() && st.send_buf.is_empty()
            };
            if drained || since.elapsed() > LINGER_TIMEOUT {
                break;
            }
        }

        let sleep = async {
            match deadline {
                Some(d) => {
                    let now = Instant::now();
                    if d > now {
                        tokio::time::sleep(d - now).await;
                    }
                }
                None => std::future::pending::<()>().await,
            }
        };

        tokio::select! {
            pkt = cfg.incoming.recv() => {
                match pkt {
                    Some((header, payload)) => {
                        let mut st = shared.state.lock();
                        st.handle_packet(&header, &payload);
                        // Drain whatever else already arrived so one ack covers the batch
                        for _ in 0..ACK_BATCH {
                            let Ok((header, payload)) = cfg.incoming.try_recv() else {
                                break;
                            };
                            st.handle_packet(&header, &payload);
                        }
                    }
                    None => {
                        // Router dropped our channel; nothing more will arrive
                        shared.state.lock().fail(io::ErrorKind::ConnectionAborted);
                    }
                }
            }
            _ = shared.nudge.notified() => {
                shared.state.lock().note_window_update();
            }
            _ = sleep => {
                shared.state.lock().check_timers();
            }
        }

        // Do per-iteration work: drain app writes, emit FIN if requested, send a standalone ack if we owe one
        {
            let mut st = shared.state.lock();
            st.fill_send_window();
            st.maybe_send_fin();
            st.fill_send_window();
            if st.needs_ack && st.state != State::SynSent {
                let ack = st.encode_state();
                st.outbox.push(ack);
                st.needs_ack = false;
            }
            st.maybe_finish();
        }
        flush(&shared, &cfg).await;
    }

    remove_connection_registration(&cfg.registry, cfg.key, &cfg.token);
    if let Some(proxy_registry) = &cfg.proxy_registry {
        remove_proxy_connection_registration(proxy_registry, cfg.key.1, &cfg.token);
    }
}

/// Drain the outbox to the wire. Datagrams are collected under the lock and sent after releasing it so UDP I/O never blocks the state mutex
async fn flush(shared: &Arc<Shared>, cfg: &DriverConfig) {
    let (datagrams, probe, mtu_floor) = {
        let mut st = shared.state.lock();
        if st
            .send_retry_at
            .is_some_and(|retry_at| retry_at > Instant::now())
        {
            return;
        }
        st.send_retry_at = None;
        (
            std::mem::take(&mut st.outbox),
            st.probe_out.take(),
            st.mtu_floor,
        )
    };
    if let Some((seq, bytes)) = &probe {
        let sent = match &cfg.transport {
            DatagramTransport::Direct(udp, Some(df)) => {
                df.send_to(udp, bytes, cfg.remote, true).await
            }
            _ => Err(io::Error::from(io::ErrorKind::Unsupported)),
        };
        if let Err(error) = sent {
            if !is_message_too_big(&error) {
                tracing::debug!("µTP MTU probe to {} failed: {error}", cfg.remote);
            }
            shared.state.lock().mtu_probe_send_failed(*seq);
        }
    }
    let mut datagrams = datagrams.into_iter();
    while let Some(d) = datagrams.next() {
        let result = match &cfg.transport {
            // Oversized (a resent probe): send under the DF lock so it can't inherit a probe's DF bit
            DatagramTransport::Direct(udp, Some(df)) if d.len() > mtu_floor => {
                df.send_to(udp, &d, cfg.remote, false).await
            }
            DatagramTransport::Direct(udp, _) => udp.send_to(&d, cfg.remote).await.map(|_| ()),
            DatagramTransport::Proxy(proxy) => proxy
                .send_to(&d, cfg.remote)
                .await
                .map(|_| ())
                .map_err(proxy_error_to_io),
        };
        if let Err(error) = result {
            let mut st = shared.state.lock();
            if is_recoverable_send_error(&error) {
                let mut pending = vec![d];
                pending.extend(datagrams);
                if st.retry_failed_datagrams(pending) {
                    return;
                }
                st.fail(error.kind());
            } else {
                st.fail(error.kind());
            }
            return;
        }
        shared.state.lock().record_send_success();
    }
}

fn proxy_error_to_io(error: HttpError) -> io::Error {
    match error {
        HttpError::Io(error) => error,
        error => io::Error::other(error.to_string()),
    }
}

fn is_recoverable_send_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock
            | io::ErrorKind::Interrupted
            | io::ErrorKind::TimedOut
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::NetworkUnreachable
            | io::ErrorKind::HostUnreachable
            | io::ErrorKind::NetworkDown
    )
}

/// A µTP connection presented as an async byte stream. Plugs into the peer connection layer wherever a `TcpStream` would go
pub struct UtpStream {
    shared: Arc<Shared>,
}

impl UtpStream {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        Self { shared }
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.shared.state.lock().remote
    }

    /// Largest datagram proven to cross the path so far
    #[cfg(test)]
    pub(crate) fn mtu_floor(&self) -> usize {
        self.shared.state.lock().mtu_floor
    }
}

impl std::fmt::Debug for UtpStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UtpStream")
            .field("peer", &self.peer_addr())
            .finish()
    }
}

impl AsyncRead for UtpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut st = self.shared.state.lock();
        if !st.recv_ready.is_empty() {
            let n = st.recv_ready.len().min(buf.remaining());
            let (first, second) = st.recv_ready.as_slices();
            let first_n = first.len().min(n);
            buf.put_slice(&first[..first_n]);
            if first_n < n {
                buf.put_slice(&second[..n - first_n]);
            }
            st.recv_ready.drain(..n);
            // Reading frees receive-buffer space; the peer learns the larger window on our next outgoing packet, so nudge a fresh ack
            self.shared.nudge.notify_one();
            return Poll::Ready(Ok(()));
        }
        if let Some(kind) = st.error {
            return Poll::Ready(Err(io::Error::from(kind)));
        }
        if st.eof {
            return Poll::Ready(Ok(())); // clean EOF (empty read)
        }
        st.read_waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl AsyncWrite for UtpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut st = self.shared.state.lock();
        if let Some(kind) = st.error {
            return Poll::Ready(Err(io::Error::from(kind)));
        }
        if matches!(st.state, State::FinSent | State::Closed) || st.want_fin {
            return Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)));
        }
        let room = SEND_BUF_MAX.saturating_sub(st.send_buf.len());
        if room == 0 {
            st.write_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let n = data.len().min(room);
        st.send_buf.extend(&data[..n]);
        drop(st);
        self.shared.nudge.notify_one();
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut st = self.shared.state.lock();
        if let Some(kind) = st.error {
            return Poll::Ready(Err(io::Error::from(kind)));
        }
        if st.send_buf.is_empty() && st.bytes_in_flight() == 0 {
            return Poll::Ready(Ok(()));
        }
        st.write_waker = Some(cx.waker().clone());
        drop(st);
        self.shared.nudge.notify_one();
        Poll::Pending
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut st = self.shared.state.lock();
        if let Some(kind) = st.error {
            return Poll::Ready(Err(io::Error::from(kind)));
        }
        if st.state == State::Closed {
            return Poll::Ready(Ok(()));
        }
        st.want_fin = true;
        // Shutdown completes once the FIN has been sent and acked (no more unacked packets) and the send buffer is empty
        if st.state == State::FinSent && st.unacked.is_empty() {
            return Poll::Ready(Ok(()));
        }
        st.write_waker = Some(cx.waker().clone());
        drop(st);
        self.shared.nudge.notify_one();
        Poll::Pending
    }
}

impl Drop for UtpStream {
    fn drop(&mut self) {
        let mut st = self.shared.state.lock();
        st.want_fin = true;
        st.read_waker = None;
        st.write_waker = None;
        drop(st);
        // Wake the driver so it can emit a FIN and tear down cleanly
        self.shared.nudge.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seq_after_handles_wraparound() {
        assert!(seq_after(5, 4));
        assert!(!seq_after(4, 5));
        assert!(!seq_after(4, 4));
        // Wraparound: 1 is after 65535
        assert!(seq_after(1, 65535));
        assert!(!seq_after(65535, 1));
    }

    #[test]
    fn fail_clears_in_flight_so_driver_stops_spinning() {
        let shared = new_shared("127.0.0.1:1".parse().unwrap(), 2, RoleKind::Initiator);
        let mut st = shared.state.lock();
        st.state = State::Connected;
        st.unacked.push_back(OutPacket {
            packet_type: PacketType::Data,
            seq_nr: 2,
            payload: vec![0u8; MSS],
            sent_at: Instant::now() - Duration::from_secs(60),
            transmissions: MAX_RETRANSMITS,
            need_resend: false,
            fast_resent: false,
        });
        st.send_buf.extend(std::iter::repeat_n(0u8, 100));

        st.fail(io::ErrorKind::TimedOut);
        assert!(st.unacked.is_empty());
        assert!(st.send_buf.is_empty());
        assert!(st.next_deadline().is_none());
    }

    #[test]
    fn transient_send_errors_remain_retriable() {
        assert!(is_recoverable_send_error(&io::Error::from(
            io::ErrorKind::ConnectionRefused,
        )));
        assert!(is_recoverable_send_error(&io::Error::from(
            io::ErrorKind::NetworkUnreachable,
        )));
        assert!(!is_recoverable_send_error(&io::Error::from(
            io::ErrorKind::InvalidData,
        )));
    }

    #[test]
    fn proxy_io_errors_preserve_their_retry_kind() {
        let error = proxy_error_to_io(HttpError::Io(io::Error::from(io::ErrorKind::WouldBlock)));
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert!(is_recoverable_send_error(&error));
    }

    #[test]
    fn retrying_unsent_datagrams_arms_a_deadline_and_preserves_order() {
        let shared = new_shared("127.0.0.1:1".parse().unwrap(), 2, RoleKind::Responder);
        let mut st = shared.state.lock();
        assert!(st.unacked.is_empty());
        st.outbox = vec![vec![3]];

        assert!(st.retry_failed_datagrams(vec![vec![1], vec![2]]));
        assert!(st.next_deadline().is_some());
        assert_eq!(st.send_retry_count, 1);
        assert_eq!(st.outbox, vec![vec![1], vec![2], vec![3]]);
    }

    fn connected_initiator() -> Arc<Shared> {
        let shared = new_shared("127.0.0.1:1".parse().unwrap(), 2, RoleKind::Initiator);
        {
            let mut st = shared.state.lock();
            st.state = State::Connected;
            st.ack_nr = 100;
            st.max_window = 64 * MSS;
            st.slow_start = false;
        }
        shared
    }

    fn ack(ack_nr: u16, selective_ack: Option<Vec<u8>>) -> UtpHeader {
        UtpHeader {
            packet_type: PacketType::State,
            connection_id: 1,
            timestamp_micros: now_micros(),
            timestamp_diff_micros: 0,
            wnd_size: RECV_BUF_MAX as u32,
            seq_nr: 101,
            ack_nr,
            selective_ack,
        }
    }

    /// Queue `n` full DATA packets (seq 2..2+n) and clear the outbox
    fn send_packets(st: &mut ConnState, n: usize) {
        st.send_buf.extend(std::iter::repeat_n(7u8, n * MSS));
        st.fill_send_window();
        assert_eq!(st.unacked.len(), n);
        st.outbox.clear();
    }

    #[test]
    fn three_duplicate_acks_fast_retransmit_the_next_packet() {
        let shared = connected_initiator();
        let mut st = shared.state.lock();
        send_packets(&mut st, 6);
        let window = st.max_window;
        // seq 2 acked, seq 3 lost: the peer keeps acking 2
        st.handle_packet(&ack(2, None), &[]);
        assert!(st.outbox.is_empty());
        st.handle_packet(&ack(2, None), &[]);
        st.handle_packet(&ack(2, None), &[]);
        assert!(st.outbox.is_empty());
        st.handle_packet(&ack(2, None), &[]);
        assert_eq!(st.outbox.len(), 1, "third duplicate ack resends seq 3");
        let (resent, _) = UtpHeader::decode(&st.outbox[0]).unwrap();
        assert_eq!(resent.seq_nr, 3);
        assert_eq!(st.max_window, window / 2);
        // A further duplicate doesn't resend again or halve again
        st.outbox.clear();
        st.handle_packet(&ack(2, None), &[]);
        assert!(st.outbox.is_empty());
        assert_eq!(st.max_window, window / 2);
    }

    #[test]
    fn selective_ack_needs_three_packets_past_a_hole() {
        let shared = connected_initiator();
        let mut st = shared.state.lock();
        send_packets(&mut st, 6);
        // Bit i acks seq 3+i: with only seq 3 and 4 SACKed, seq 2 has two packets after it
        st.handle_packet(&ack(1, Some(vec![0b0000_0011, 0, 0, 0])), &[]);
        assert!(st.outbox.is_empty());
        assert_eq!(st.unacked.len(), 4);
        // seq 5 SACKed too: three packets past seq 2, which is now presumed lost
        st.handle_packet(&ack(1, Some(vec![0b0000_0111, 0, 0, 0])), &[]);
        let resent: Vec<u16> = st
            .outbox
            .iter()
            .map(|d| UtpHeader::decode(d).unwrap().0.seq_nr)
            .collect();
        assert_eq!(resent, vec![2]);
    }

    #[test]
    fn ack_for_unsent_packet_is_ignored() {
        let shared = connected_initiator();
        let mut st = shared.state.lock();
        send_packets(&mut st, 2);
        // We sent seq 2 and 3; an ack of 500 is forged
        st.handle_packet(&ack(500, None), &[]);
        assert_eq!(st.unacked.len(), 2);
    }

    #[test]
    fn timeout_collapses_window_and_resends_as_it_reopens() {
        let shared = connected_initiator();
        let mut st = shared.state.lock();
        send_packets(&mut st, 4);
        st.rto_base = Some(Instant::now() - Duration::from_secs(5));
        st.check_timers();
        assert_eq!(st.max_window, MIN_WINDOW);
        assert_eq!(st.rto, INITIAL_RTO * 2);
        // Only the oldest is back in flight; the rest wait for the window
        assert_eq!(st.outbox.len(), 1);
        assert_eq!(st.bytes_in_flight(), MSS);
        assert!(st.unacked.iter().skip(1).all(|p| p.need_resend));
        st.outbox.clear();
        st.fill_send_window();
        assert!(st.outbox.is_empty(), "a 150-byte window holds one packet");
        // Ack of the resent packet lets the next one go
        st.handle_packet(&ack(2, None), &[]);
        st.fill_send_window();
        assert_eq!(st.outbox.len(), 1);
        assert_eq!(UtpHeader::decode(&st.outbox[0]).unwrap().0.seq_nr, 3);
    }

    #[test]
    fn zero_receive_window_is_probed() {
        let shared = connected_initiator();
        let mut st = shared.state.lock();
        st.peer_wnd = 0;
        st.send_buf.extend([1u8; 10]);
        st.fill_send_window();
        assert!(st.outbox.is_empty());
        let probe_at = st.probe_at.expect("probe armed");
        assert!(st.next_deadline().is_some_and(|d| d <= probe_at));
        st.probe_at = Some(Instant::now() - Duration::from_millis(1));
        st.check_timers();
        st.fill_send_window();
        assert_eq!(st.outbox.len(), 1, "one probe packet");
        assert!(!st.probe_due);
    }

    #[test]
    fn duplicate_syn_is_reacknowledged() {
        let shared = new_shared("127.0.0.1:1".parse().unwrap(), 9, RoleKind::Responder);
        let mut st = shared.state.lock();
        let mut syn = ack(0, None);
        syn.packet_type = PacketType::Syn;
        st.handle_packet(&syn, &[]);
        assert!(st.needs_ack);
    }

    #[test]
    fn acked_fin_wakes_shutdown() {
        let shared = connected_initiator();
        let waker = std::sync::Arc::new(CountingWaker::default());
        let mut st = shared.state.lock();
        st.want_fin = true;
        st.maybe_send_fin();
        st.write_waker = Some(std::task::Waker::from(waker.clone()));
        // The FIN (seq 2, no payload) is acked
        st.handle_packet(&ack(2, None), &[]);
        assert!(st.unacked.is_empty());
        assert_eq!(waker.wakes.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[derive(Default)]
    struct CountingWaker {
        wakes: std::sync::atomic::AtomicUsize,
    }

    impl std::task::Wake for CountingWaker {
        fn wake(self: std::sync::Arc<Self>) {
            self.wakes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[test]
    fn nagle_coalesces_small_writes_while_data_is_in_flight() {
        let shared = connected_initiator();
        let mut st = shared.state.lock();
        // Nothing in flight: a small write goes out at once
        st.send_buf.extend([1u8; 10]);
        st.fill_send_window();
        assert_eq!(st.outbox.len(), 1);
        st.outbox.clear();
        // With that packet unacked, further small writes wait and merge
        st.send_buf.extend([2u8; 10]);
        st.fill_send_window();
        st.send_buf.extend([3u8; 10]);
        st.fill_send_window();
        assert!(st.outbox.is_empty());
        // The ack releases them as one packet
        st.handle_packet(&ack(2, None), &[]);
        st.fill_send_window();
        assert_eq!(st.outbox.len(), 1);
        let (_, payload) = UtpHeader::decode(&st.outbox[0]).unwrap();
        assert_eq!(payload.len(), 20);
        st.outbox.clear();
        // A full packet is never held, and closing flushes a partial one
        st.send_buf.extend(std::iter::repeat_n(4u8, MSS + 5));
        st.fill_send_window();
        assert_eq!(st.outbox.len(), 1);
        st.want_fin = true;
        st.fill_send_window();
        assert_eq!(st.outbox.len(), 2);
    }

    /// Connected initiator with path-MTU discovery on and a wide window
    fn probing_initiator() -> Arc<Shared> {
        let shared = connected_initiator();
        {
            let mut st = shared.state.lock();
            st.pmtud = true;
            st.max_window = 256 * MSS;
        }
        shared
    }

    #[test]
    fn mtu_probe_ack_raises_the_floor() {
        let shared = probing_initiator();
        let mut st = shared.state.lock();
        let (floor, ceiling) = (st.mtu_floor, st.mtu_ceiling);
        assert_eq!((floor, ceiling), (MSS + UTP_OVERHEAD, MAX_DATAGRAM_V4));
        st.send_buf.extend(std::iter::repeat_n(9u8, 8 * MSS));
        st.fill_send_window();
        let (probe_seq, probe_bytes) = st.probe_out.clone().expect("probe queued");
        assert_eq!(probe_seq, 2, "the first data packet is the probe");
        assert!(probe_bytes.len() > floor && probe_bytes.len() <= (floor + ceiling) / 2);
        // Normal packets keep the proven size while the probe is out
        assert!(st.outbox.iter().all(|d| d.len() <= floor));
        st.handle_packet(&ack(probe_seq, None), &[]);
        assert_eq!(st.mtu_floor, probe_bytes.len());
        assert_eq!(st.packet_size, st.mtu_floor - UTP_OVERHEAD);
        assert!(st.mtu_probe.is_none());
    }

    #[test]
    fn lost_mtu_probe_lowers_the_ceiling_without_cutting_the_window() {
        let shared = probing_initiator();
        let mut st = shared.state.lock();
        st.send_buf.extend(std::iter::repeat_n(9u8, 8 * MSS));
        st.fill_send_window();
        let (probe_seq, probe_bytes) = st.probe_out.take().unwrap();
        st.outbox.clear();
        let window = st.max_window;
        // SACK seq 3, 4 and 5
        st.handle_packet(&ack(1, Some(vec![0b0000_0111, 0, 0, 0])), &[]);
        assert_eq!(st.mtu_ceiling, probe_bytes.len() - 1);
        assert!(st.max_window >= window, "a lost probe isn't congestion");
        let resent: Vec<u16> = st
            .outbox
            .iter()
            .map(|d| UtpHeader::decode(d).unwrap().0.seq_nr)
            .collect();
        assert_eq!(
            resent,
            vec![probe_seq],
            "resent as a normal datagram (no don't-fragment)"
        );
        assert!(st.probe_out.is_none());
    }

    #[test]
    fn probe_timing_out_alone_counts_as_too_big() {
        let shared = probing_initiator();
        let mut st = shared.state.lock();
        st.set_mtu_floor(500 + UTP_OVERHEAD);
        // Exactly one probe's worth of data, so the probe is the only packet in flight
        let payload = st.probe_payload();
        st.send_buf.extend(std::iter::repeat_n(9u8, payload));
        st.fill_send_window();
        let (_, probe_bytes) = st.probe_out.take().expect("probe queued");
        assert_eq!(st.unacked.len(), 1);
        let window = st.max_window;
        st.rto_base = Some(Instant::now() - Duration::from_secs(5));
        st.check_timers();
        assert_eq!(st.mtu_ceiling, probe_bytes.len() - 1);
        assert_eq!(
            st.max_window, window,
            "no timeout collapse for a lone probe"
        );
        assert_eq!(st.outbox.len(), 1);
    }

    #[test]
    fn refused_probe_is_resent_immediately() {
        let shared = probing_initiator();
        let mut st = shared.state.lock();
        st.send_buf.extend(std::iter::repeat_n(9u8, 8 * MSS));
        st.fill_send_window();
        let (seq, bytes) = st.probe_out.take().unwrap();
        st.outbox.clear();
        st.mtu_probe_send_failed(seq);
        assert_eq!(st.mtu_ceiling, bytes.len() - 1);
        assert_eq!(st.outbox.len(), 1);
        assert_eq!(UtpHeader::decode(&st.outbox[0]).unwrap().0.seq_nr, seq);
    }

    #[test]
    fn mtu_search_restarts_when_a_known_size_fails() {
        let shared = probing_initiator();
        let mut st = shared.state.lock();
        st.mtu_floor = 1400;
        st.mtu_ceiling = 1300;
        st.update_mtu_limits();
        assert_eq!(st.mtu_ceiling, 1400);
        assert_eq!(st.mtu_floor, (MIN_DATAGRAM_V4 + 1400) / 2);
        assert_eq!(st.packet_size, st.mtu_floor - UTP_OVERHEAD);
    }

    #[test]
    fn no_probes_without_dont_fragment_support() {
        let shared = connected_initiator();
        let mut st = shared.state.lock();
        st.max_window = 256 * MSS;
        st.send_buf.extend(std::iter::repeat_n(9u8, 8 * MSS));
        st.fill_send_window();
        assert!(st.probe_out.is_none());
    }

    #[test]
    fn selective_ack_mask_grows_past_32_packets() {
        let shared = connected_initiator();
        let mut st = shared.state.lock();
        // ack_nr = 100; buffer seq 102 and 102 + 70
        st.reorder.insert(102, vec![1]);
        st.reorder.insert(172, vec![1]);
        let mask = st.build_selective_ack().unwrap();
        assert_eq!(mask.len(), 12);
        assert_eq!(mask[0] & 1, 1);
        assert_ne!(mask[70 / 8] & (1 << (70 % 8)), 0);
        // Packets past the 256-bit cap are left out
        st.reorder.insert(102 + 400, vec![1]);
        assert_eq!(st.build_selective_ack().unwrap().len(), 12);
    }

    #[test]
    fn far_future_packets_are_not_buffered() {
        let shared = connected_initiator();
        let mut st = shared.state.lock();
        let mut data = ack(1, None);
        data.packet_type = PacketType::Data;
        data.seq_nr = 100u16.wrapping_add(MAX_REORDER_DISTANCE + 5);
        st.handle_packet(&data, b"x");
        assert!(st.reorder.is_empty());
        assert!(st.needs_ack);
    }

    #[test]
    fn draining_a_full_receive_buffer_sends_a_window_update() {
        let shared = connected_initiator();
        let mut st = shared.state.lock();
        st.recv_ready.extend(std::iter::repeat_n(0u8, RECV_BUF_MAX));
        let _ = st.encode_state();
        assert_eq!(st.last_advertised.get(), 0);
        st.recv_ready.clear();
        st.note_window_update();
        assert!(st.needs_ack);
    }

    #[test]
    fn base_delay_forgets_minima_older_than_two_minutes() {
        let mut hist = DelayHistory::default();
        let t0 = Instant::now();
        assert_eq!(hist.add_sample(1_000, t0), 0);
        // Queuing builds up on top of the 1 ms base
        assert_eq!(
            hist.add_sample(51_000, t0),
            0,
            "min of recent samples still 1 ms"
        );
        hist.add_sample(51_000, t0);
        assert_eq!(hist.add_sample(51_000, t0), 50_000);
        // Three minutes of 20 ms samples age out the 1 ms minimum
        for minute in 1..=3 {
            hist.add_sample(20_000, t0 + BASE_DELAY_BUCKET * minute);
        }
        hist.add_sample(20_000, t0 + BASE_DELAY_BUCKET * 3);
        assert_eq!(hist.add_sample(20_000, t0 + BASE_DELAY_BUCKET * 3), 0);
        // Wrapping timestamps compare correctly
        assert!(wrapping_lt(u32::MAX - 5, 3));
    }

    #[test]
    fn ledbat_grows_below_target_and_shrinks_above_it() {
        let shared = connected_initiator();
        let mut st = shared.state.lock();
        let window = st.max_window;
        // Window-limited with no queuing delay: grows by at most 3000 bytes
        st.update_cwnd(1_000, window, window);
        assert!(st.max_window > window && st.max_window <= window + 3000);
        // Sustained queuing above target shrinks it; one spike is filtered out
        let grown = st.max_window;
        st.update_cwnd(400_000, grown, grown);
        assert!(st.max_window >= grown, "one high sample is filtered");
        for _ in 0..CUR_DELAY_SAMPLES {
            st.update_cwnd(400_000, grown, grown);
        }
        assert!(st.max_window < grown);
        // Not window-limited: no growth even below target
        let current = st.max_window;
        st.update_cwnd(1_000, MSS, 0);
        assert_eq!(st.max_window, current);
    }
}
