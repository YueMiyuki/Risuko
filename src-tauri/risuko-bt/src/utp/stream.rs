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
use tokio::sync::{mpsc, oneshot, Notify};

use risuko_http::{Error as HttpError, ProxyAssociation};

use super::dontfrag::{is_message_too_big, UdpSender};
use super::now_micros;
use super::packet::{PacketType, UtpHeader, HEADER_LEN};
use super::socket::{
    remove_connection_registration, remove_proxy_connection_registration, ConnKey, ConnRegistry,
    ConnectionToken, ProxyConnRegistry,
};
const MSS: usize = 1200;
const UTP_OVERHEAD: usize = HEADER_LEN + 2 + MAX_SACK_BYTES;
const MIN_DATAGRAM_V6: usize = 1232;
const MIN_DATAGRAM_V4: usize = 548;
const MAX_DATAGRAM_V4: usize = 1472;
const MAX_DATAGRAM_V6: usize = 1452;
const MTU_SEARCH_GRANULARITY: usize = 16;
const MTU_REPROBE_INTERVAL: Duration = Duration::from_secs(600);
const RECV_BUF_MAX: usize = 1024 * 1024;
const SEND_BUF_MAX: usize = 512 * 1024;
const TARGET_MICROS: f64 = 100_000.0;
const MAX_CWND_INCREASE_BYTES_PER_RTT: f64 = 3000.0;
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
const ACK_BATCH: usize = 64;
const DUP_ACK_THRESHOLD: usize = 3;
const MAX_SACK_BYTES: usize = 32;
const MAX_REORDER_DISTANCE: u16 = 2048;
const BASE_DELAY_BUCKET: Duration = Duration::from_secs(60);
const BASE_DELAY_BUCKETS: usize = 2;
const CUR_DELAY_SAMPLES: usize = 3;
const MIN_ZERO_WINDOW_PROBE: Duration = Duration::from_secs(1);
const MIN_BURST_PACKETS: usize = 16;
const MIN_PACE_INTERVAL: Duration = Duration::from_millis(1);
const MAX_PACE_INTERVAL: Duration = Duration::from_millis(100);
const MAXED_OUT_WINDOW_MEMORY: Duration = Duration::from_secs(1);
pub(crate) const INCOMING_QUEUE_PACKETS: usize = 1024;

fn seq_after(a: u16, b: u16) -> bool {
    let d = a.wrapping_sub(b);
    d != 0 && d < 0x8000
}

fn wrapping_lt(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) < 0
}

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

struct OutPacket {
    packet_type: PacketType,
    seq_nr: u16,
    payload: Vec<u8>,
    sent_at: Instant,
    transmissions: u32,
    need_resend: bool,
    fast_resent: bool,
}

#[derive(Default)]
struct DelayHistory {
    history: VecDeque<u32>,
    bucket_min: Option<u32>,
    bucket_start: Option<Instant>,
    recent: VecDeque<u32>,
}

impl DelayHistory {
    fn add_sample(&mut self, sample: u32, now: Instant) -> u32 {
        match (self.bucket_start, self.bucket_min) {
            (Some(start), Some(min)) if now.duration_since(start) >= BASE_DELAY_BUCKET => {
                self.history.push_back(min);
                while self.history.len() >= BASE_DELAY_BUCKETS {
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
    packet_size: usize,
    pmtud: bool,
    mtu_floor: usize,
    mtu_ceiling: usize,
    mtu_probe: Option<(u16, usize)>,
    probe_out: Option<(u16, Vec<u8>)>,
    mtu_reprobe_at: Instant,

    seq_nr: u16,
    ack_nr: u16,

    send_buf: VecDeque<u8>,
    unacked: VecDeque<OutPacket>,
    in_flight: usize,
    resend_count: usize,

    recv_ready: VecDeque<u8>,
    reorder: BTreeMap<u16, Vec<u8>>,
    reorder_bytes: usize,

    peer_wnd: u32,
    max_window: usize,
    slow_start: bool,
    ss_thresh: usize,
    delay: DelayHistory,

    rtt: f64,
    rtt_var: f64,
    rto: Duration,
    rto_base: Option<Instant>,
    reply_micros: u32,

    last_ack: Option<u16>,
    dup_acks: usize,
    probe_at: Option<Instant>,
    probe_due: bool,
    last_advertised: std::cell::Cell<u32>,

    needs_ack: bool,
    want_fin: bool,
    peer_fin: Option<u16>,
    eof: bool,
    error: Option<io::ErrorKind>,

    recovery_seq: Option<u16>,
    last_maxed_out: Option<Instant>,
    pace_at: Option<Instant>,
    fin_acked_at: Option<Instant>,

    outbox: Vec<Vec<u8>>,
    send_retry_at: Option<Instant>,
    send_retry_count: u32,
    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
    connect_notify: Option<oneshot::Sender<io::Result<()>>>,
}

impl ConnState {
    fn advertised_window(&self) -> u32 {
        let used = self.recv_ready.len().saturating_add(self.reorder_bytes);
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

    fn encode(&self, p: &OutPacket) -> Vec<u8> {
        let conn_id = if p.packet_type == PacketType::Syn {
            self.conn_id_send.wrapping_sub(1)
        } else {
            self.conn_id_send
        };
        self.header(p.packet_type, conn_id, p.seq_nr)
            .encode(&p.payload)
    }

    fn encode_state(&self) -> Vec<u8> {
        self.header(PacketType::State, self.conn_id_send, self.seq_nr)
            .encode(&[])
    }

    fn build_selective_ack(&self) -> Option<Vec<u8>> {
        if self.reorder.is_empty() {
            return None;
        }
        let base = self.ack_nr.wrapping_add(2);
        let max_bits = MAX_SACK_BYTES * 8;
        let mut mask = [0u8; MAX_SACK_BYTES];
        let mut highest: Option<usize> = None;
        for &seq in self.reorder.keys() {
            let bit = seq.wrapping_sub(base) as usize;
            if bit < max_bits {
                mask[bit / 8] |= 1 << (bit % 8);
                highest = highest.max(Some(bit));
            }
        }
        Some(mask[..(highest? / 32 + 1) * 4].to_vec())
    }

    fn bytes_in_flight(&self) -> usize {
        self.in_flight
    }

    fn account_removed(&mut self, p: &OutPacket) {
        if p.need_resend {
            self.resend_count = self.resend_count.saturating_sub(1);
        } else {
            self.in_flight = self.in_flight.saturating_sub(p.payload.len());
        }
    }

    fn may_fast_resend(&self, p: &OutPacket) -> bool {
        !p.fast_resent || p.sent_at.elapsed() >= self.rto
    }

    fn burst_limit(&self) -> usize {
        if self.rtt <= 0.0 {
            return usize::MAX;
        }
        let window_packets = (self.max_window / self.packet_size.max(1)).max(1) as f64;
        let per_timer_tick = (window_packets * MIN_PACE_INTERVAL.as_secs_f64() / self.rtt).ceil();
        (per_timer_tick as usize).max(MIN_BURST_PACKETS)
    }

    fn pace_interval(&self, burst: usize) -> Duration {
        let window_packets = (self.max_window / self.packet_size.max(1)).max(1) as f64;
        Duration::from_secs_f64(self.rtt * burst as f64 / window_packets)
            .clamp(MIN_PACE_INTERVAL, MAX_PACE_INTERVAL)
    }

    fn note_cwnd_block(&mut self, len: usize) {
        let in_flight = self.in_flight;
        if in_flight > 0
            && in_flight + len <= self.peer_wnd as usize
            && in_flight + len > self.max_window
        {
            self.last_maxed_out = Some(Instant::now());
        }
    }

    fn take_send(&mut self, n: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(n);
        let (head, tail) = self.send_buf.as_slices();
        let first = head.len().min(n);
        out.extend_from_slice(&head[..first]);
        if first < n {
            out.extend_from_slice(&tail[..n - first]);
        }
        self.send_buf.drain(..n);
        out
    }

    fn may_send(&self, len: usize) -> bool {
        let in_flight = self.bytes_in_flight();
        let fits_peer = in_flight + len <= self.peer_wnd as usize;
        if in_flight == 0 {
            return fits_peer || self.probe_due;
        }
        fits_peer && in_flight + len <= self.max_window
    }

    fn fill_send_window(&mut self) {
        if self.state == State::SynSent || self.state == State::Closed {
            return;
        }
        if self.pace_at.is_some_and(|at| Instant::now() < at) {
            return;
        }
        self.pace_at = None;
        let burst = self.burst_limit();
        let mut sent = 0usize;
        let mut paced = false;
        while self.resend_count > 0 {
            let Some(idx) = self.unacked.iter().position(|p| p.need_resend) else {
                break;
            };
            let len = self.unacked[idx].payload.len();
            if !self.may_send(len) {
                self.note_cwnd_block(len);
                break;
            }
            if sent >= burst {
                paced = true;
                break;
            }
            self.retransmit(idx);
            sent += 1;
        }
        if self.state == State::Connected && !paced {
            if !self.send_buf.is_empty() {
                self.maybe_reopen_mtu_search();
            }
            while !self.send_buf.is_empty() {
                if self.should_probe() {
                    let payload = self.probe_payload();
                    if self.send_buf.len() >= payload && self.may_send(payload) {
                        let payload = self.take_send(payload);
                        self.transmit_probe(payload);
                        sent += 1;
                        continue;
                    }
                }
                let take = self.send_buf.len().min(self.packet_size);
                if take < self.packet_size && self.bytes_in_flight() > 0 && !self.want_fin {
                    break;
                }
                if !self.may_send(take) {
                    self.note_cwnd_block(take);
                    break;
                }
                if sent >= burst {
                    paced = true;
                    break;
                }
                let payload = self.take_send(take);
                self.transmit_new(PacketType::Data, payload);
                sent += 1;
            }
        }
        if paced {
            let now = Instant::now();
            self.pace_at = Some(now + self.pace_interval(burst));
            self.last_maxed_out = Some(now);
        }
        let blocked = !self.send_buf.is_empty() || self.resend_count > 0;
        if blocked && self.bytes_in_flight() == 0 && self.probe_at.is_none() && !self.probe_due {
            self.probe_at = Some(Instant::now() + self.rto.max(MIN_ZERO_WINDOW_PROBE));
        }
    }

    fn arm_rto(&mut self) {
        if self.rto_base.is_none() {
            self.rto_base = Some(Instant::now());
        }
    }

    fn set_mtu_floor(&mut self, floor: usize) {
        self.mtu_floor = floor;
        self.packet_size = floor - UTP_OVERHEAD;
    }

    fn update_mtu_limits(&mut self) {
        if self.mtu_floor > self.mtu_ceiling {
            self.mtu_ceiling = self.mtu_floor;
            let floor = (min_datagram(self.remote) + self.mtu_ceiling) / 2;
            self.set_mtu_floor(floor);
        }
        self.mtu_probe = None;
    }

    fn maybe_reopen_mtu_search(&mut self) {
        if !self.pmtud {
            return;
        }
        let now = Instant::now();
        if now >= self.mtu_reprobe_at {
            self.mtu_reprobe_at = now + MTU_REPROBE_INTERVAL;
            self.mtu_ceiling = self.mtu_ceiling.max(max_datagram(self.remote));
        }
    }

    fn should_probe(&self) -> bool {
        self.pmtud
            && self.state == State::Connected
            && self.mtu_probe.is_none()
            && self.probe_out.is_none()
            && self.mtu_ceiling >= self.mtu_floor + MTU_SEARCH_GRANULARITY
            && self.max_window > 3 * self.mtu_floor
    }

    fn probe_payload(&self) -> usize {
        let sack = self.build_selective_ack().map_or(0, |mask| 2 + mask.len());
        (self.mtu_floor + self.mtu_ceiling) / 2 - HEADER_LEN - sack
    }

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
        self.in_flight += p.payload.len();
        let bytes = self.encode(&p);
        self.mtu_probe = Some((p.seq_nr, bytes.len()));
        self.probe_out = Some((p.seq_nr, bytes));
        self.unacked.push_back(p);
        self.probe_due = false;
        self.arm_rto();
        self.needs_ack = false;
    }

    fn on_mtu_probe_acked(&mut self, seq: u16) {
        if let Some((probe, size)) = self.mtu_probe {
            if probe == seq {
                self.set_mtu_floor(self.mtu_floor.max(size));
                self.update_mtu_limits();
            }
        }
    }

    fn on_mtu_probe_lost(&mut self, seq: u16) -> bool {
        match self.mtu_probe {
            Some((probe, size)) if probe == seq && size <= self.mtu_floor => {
                self.mtu_probe = None;
                false
            }
            Some((probe, size)) if probe == seq => {
                self.mtu_ceiling = size - 1;
                self.update_mtu_limits();
                true
            }
            _ => false,
        }
    }

    pub(crate) fn mtu_probe_send_failed(&mut self, seq: u16, too_big: bool) {
        if too_big {
            self.on_mtu_probe_lost(seq);
        } else if self.mtu_probe.is_some_and(|(probe, _)| probe == seq) {
            self.mtu_probe = None;
        }
        if let Some(idx) = self.unacked.iter().position(|p| p.seq_nr == seq) {
            self.retransmit(idx);
        }
    }

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
        self.in_flight += p.payload.len();
        self.outbox.push(self.encode(&p));
        self.unacked.push_back(p);
        self.probe_due = false;
        self.arm_rto();
        self.needs_ack = false;
    }

    fn retransmit(&mut self, idx: usize) {
        let Some(p) = self.unacked.get_mut(idx) else {
            return;
        };
        p.sent_at = Instant::now();
        p.transmissions += 1;
        if p.need_resend {
            p.need_resend = false;
            self.resend_count = self.resend_count.saturating_sub(1);
            self.in_flight += p.payload.len();
        }
        let bytes = self.encode(&self.unacked[idx]);
        self.outbox.push(bytes);
        self.probe_due = false;
        self.arm_rto();
        self.needs_ack = false;
    }

    fn on_loss(&mut self) {
        if self.recovery_seq.is_none() {
            self.recovery_seq = Some(self.seq_nr.wrapping_sub(1));
            self.max_window = (self.max_window / 2).max(MIN_WINDOW);
            self.ss_thresh = self.max_window;
            self.slow_start = false;
        }
    }

    fn process_ack(&mut self, header: &UtpHeader) -> usize {
        let ack_nr = header.ack_nr;
        let mut acked_bytes = 0usize;
        let mut acked_any = false;
        while let Some(front) = self.unacked.front() {
            if seq_after(front.seq_nr, ack_nr) {
                break;
            }
            let Some(p) = self.unacked.pop_front() else {
                break;
            };
            self.account_removed(&p);
            acked_any = true;
            acked_bytes += p.payload.len();
            if p.transmissions == 1 {
                self.update_rtt(p.sent_at.elapsed());
                self.on_mtu_probe_acked(p.seq_nr);
            }
        }
        if acked_any {
            self.dup_acks = 0;
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

    fn fast_retransmit_front(&mut self, ack_nr: u16) {
        let lost = ack_nr.wrapping_add(1);
        if self
            .unacked
            .front()
            .is_some_and(|p| p.seq_nr == lost && self.may_fast_resend(p))
        {
            self.unacked[0].fast_resent = true;
            if !self.on_mtu_probe_lost(lost) {
                self.on_loss();
            }
            self.retransmit(0);
        }
    }

    fn process_selective_ack(&mut self, ack_nr: u16, mask: &[u8]) -> usize {
        let total: usize = mask.iter().map(|b| b.count_ones() as usize).sum();
        if total == 0 {
            return 0;
        }
        let base = ack_nr.wrapping_add(2);
        let nbits = mask.len() * 8;
        let bit_of = |seq: u16| seq.wrapping_sub(base) as usize;
        let is_set = |bit: usize| bit < nbits && mask[bit / 8] & (1 << (bit % 8)) != 0;
        let mut acked_bytes = 0usize;
        let mut samples = Vec::new();
        let (mut gone_in_flight, mut gone_resend) = (0usize, 0usize);
        self.unacked.retain(|p| {
            if !is_set(bit_of(p.seq_nr)) {
                return true;
            }
            acked_bytes += p.payload.len();
            if p.need_resend {
                gone_resend += 1;
            } else {
                gone_in_flight += p.payload.len();
            }
            if p.transmissions == 1 {
                samples.push((p.seq_nr, p.sent_at.elapsed()));
            }
            false
        });
        self.in_flight = self.in_flight.saturating_sub(gone_in_flight);
        self.resend_count = self.resend_count.saturating_sub(gone_resend);
        for (seq, sample) in samples {
            self.update_rtt(sample);
            self.on_mtu_probe_acked(seq);
        }
        let mut lost: Vec<usize> = Vec::new();
        let (mut cursor, mut set_below) = (0usize, 0usize);
        for (idx, p) in self.unacked.iter().enumerate() {
            if !self.may_fast_resend(p) {
                continue;
            }
            let bit = bit_of(p.seq_nr);
            let sacked_after = if bit >= 0x8000 {
                total
            } else if bit >= nbits {
                0
            } else {
                while cursor <= bit {
                    set_below += usize::from(is_set(cursor));
                    cursor += 1;
                }
                total - set_below
            };
            if sacked_after >= DUP_ACK_THRESHOLD {
                lost.push(idx);
            }
        }
        if !lost.is_empty() {
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

    fn update_cwnd(&mut self, their_delay: u32, acked_bytes: usize) {
        if acked_bytes == 0 {
            return;
        }
        let delay_factor = if their_delay == 0 {
            0.0
        } else {
            let our_delay = self.delay.add_sample(their_delay, Instant::now()) as f64;
            (TARGET_MICROS - our_delay) / TARGET_MICROS
        };
        let window = self.max_window.max(1) as f64;
        let window_factor = (acked_bytes as f64).min(window) / window.max(acked_bytes as f64);
        let mut gain = MAX_CWND_INCREASE_BYTES_PER_RTT * delay_factor * window_factor;
        let window_limited = self
            .last_maxed_out
            .is_some_and(|at| at.elapsed() < MAXED_OUT_WINDOW_MEMORY);
        if gain > 0.0 && !window_limited {
            gain = 0.0;
        }
        if self.slow_start {
            if delay_factor < 0.0 {
                self.slow_start = false;
            } else if window_limited {
                let ss_gain = gain.max(acked_bytes as f64);
                if self.max_window as f64 + ss_gain > self.ss_thresh as f64 {
                    self.slow_start = false;
                } else {
                    gain = ss_gain;
                }
            }
        }
        let next = self.max_window as f64 + gain;
        self.max_window = (next as i64).clamp(MIN_WINDOW as i64, MAX_CWND as i64) as usize;
    }

    fn handle_packet(&mut self, header: &UtpHeader, payload: &[u8]) {
        if self.state == State::Closed {
            return;
        }
        if header.packet_type == PacketType::Syn {
            if !self.is_initiator {
                self.needs_ack = true;
            }
            return;
        }
        if seq_after(header.ack_nr, self.seq_nr.wrapping_sub(1)) {
            return;
        }
        let previous_peer_wnd = self.peer_wnd;
        self.peer_wnd = header.wnd_size;
        if self.peer_wnd > previous_peer_wnd {
            self.probe_at = None;
        }
        self.reply_micros = now_micros().wrapping_sub(header.timestamp_micros);

        if self.state == State::SynSent && header.packet_type == PacketType::State {
            self.state = State::Connected;
            self.ack_nr = header.seq_nr.wrapping_sub(1);
            if let Some(tx) = self.connect_notify.take() {
                let _ = tx.send(Ok(()));
            }
            self.notify_write();
        }

        let unacked_before = self.unacked.len();
        let mut acked = self.process_ack(header);
        if let Some(mask) = &header.selective_ack {
            acked += self.process_selective_ack(header.ack_nr, mask);
        }
        self.update_cwnd(header.timestamp_diff_micros, acked);
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

        if let Some(fin) = self.peer_fin {
            if !seq_after(fin, self.ack_nr) {
                self.eof = true;
                self.notify_read();
            }
        }
        self.maybe_finish();
    }

    fn accept_inorder(&mut self, header: &UtpHeader, payload: &[u8]) {
        self.needs_ack = true;
        if self.fin_acked_at.is_some() {
            self.fin_acked_at = Some(Instant::now());
        }
        let distance = header.seq_nr.wrapping_sub(self.ack_nr);
        if distance == 0 || distance >= 0x8000 {
            return;
        }
        if distance > MAX_REORDER_DISTANCE {
            return;
        }
        if self.advertised_window() == 0 && !payload.is_empty() {
            return;
        }
        if distance == 1 {
            self.consume(header.packet_type, header.seq_nr, payload);
            loop {
                let next = self.ack_nr.wrapping_add(1);
                let Some(buf) = self.reorder.remove(&next) else {
                    break;
                };
                self.reorder_bytes = self.reorder_bytes.saturating_sub(buf.len());
                let ty = if Some(next) == self.peer_fin {
                    PacketType::Fin
                } else {
                    PacketType::Data
                };
                self.consume(ty, next, &buf);
            }
        } else {
            // Record an out-of-order FIN even when the buffer is full, or EOF can stall
            if header.packet_type == PacketType::Fin {
                self.peer_fin = Some(header.seq_nr);
            }
            if self.reorder.len() < RECV_BUF_MAX / self.packet_size {
                if let Some(old) = self.reorder.insert(header.seq_nr, payload.to_vec()) {
                    self.reorder_bytes = self.reorder_bytes.saturating_sub(old.len());
                }
                self.reorder_bytes += payload.len();
            }
        }
    }

    fn consume(&mut self, ty: PacketType, seq: u16, payload: &[u8]) {
        self.ack_nr = seq;
        if ty == PacketType::Fin {
            self.peer_fin = Some(seq);
        } else if !payload.is_empty() {
            self.recv_ready.extend(payload);
            self.notify_read();
        }
    }

    fn check_timers(&mut self) {
        let now = Instant::now();
        if let Some(at) = self.probe_at {
            if now >= at {
                self.probe_at = None;
                self.probe_due = true;
            }
        }
        if self.state == State::FinSent
            && self
                .fin_acked_at
                .is_some_and(|at| now >= at + LINGER_TIMEOUT)
        {
            self.eof = true;
            self.state = State::Closed;
            self.notify_read();
            self.notify_write();
            return;
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
        if self.unacked.len() == 1 && self.on_mtu_probe_lost(self.unacked[0].seq_nr) {
            self.rto_base = None;
            self.retransmit(0);
            return;
        }
        self.mtu_probe = None;
        self.ss_thresh = (self.max_window / 2).max(2 * self.packet_size);
        self.max_window = self.packet_size.max(MIN_WINDOW);
        self.slow_start = true;
        self.rto = (self.rto * 2).min(MAX_RTO);
        self.dup_acks = 0;
        self.recovery_seq = None;
        for p in self.unacked.iter_mut() {
            p.need_resend = true;
            p.fast_resent = false;
        }
        self.in_flight = 0;
        self.resend_count = self.unacked.len();
        self.rto_base = None;
        self.retransmit(0);
    }

    fn maybe_send_fin(&mut self) {
        if self.want_fin && self.state == State::Connected && self.send_buf.is_empty() {
            self.transmit_new(PacketType::Fin, Vec::new());
            self.state = State::FinSent;
        }
    }

    fn maybe_finish(&mut self) {
        if self.state == State::FinSent && self.unacked.is_empty() {
            if self.eof {
                self.state = State::Closed;
            } else if self.fin_acked_at.is_none() {
                self.fin_acked_at = Some(Instant::now());
            }
        }
    }

    fn note_window_update(&mut self) {
        let threshold = self.packet_size as u32;
        if self.last_advertised.get() < threshold && self.advertised_window() >= threshold {
            self.needs_ack = true;
        }
    }

    fn next_deadline(&self) -> Option<Instant> {
        let retransmit_at = self
            .rto_base
            .filter(|_| !self.unacked.is_empty())
            .map(|base| base + self.rto);
        let fin_wait = self
            .fin_acked_at
            .filter(|_| self.state == State::FinSent)
            .map(|at| at + LINGER_TIMEOUT);
        let pace = self
            .pace_at
            .filter(|_| matches!(self.state, State::Connected | State::FinSent));
        [
            retransmit_at,
            self.probe_at,
            self.send_retry_at,
            fin_wait,
            pace,
        ]
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
        self.clear_unacked();
        self.send_buf.clear();
        self.rto_base = None;
        self.probe_at = None;
        self.pace_at = None;
        if let Some(tx) = self.connect_notify.take() {
            let _ = tx.send(Err(io::Error::from(kind)));
        }
        self.notify_read();
        self.notify_write();
    }

    pub(crate) fn seed_responder(&mut self, syn: &UtpHeader) {
        self.ack_nr = syn.seq_nr;
        self.peer_wnd = syn.wnd_size;
        self.reply_micros = now_micros().wrapping_sub(syn.timestamp_micros);
    }

    pub(crate) fn force_close(&mut self) {
        self.state = State::Closed;
        self.error.get_or_insert(io::ErrorKind::TimedOut);
        self.clear_unacked();
        self.send_buf.clear();
        self.rto_base = None;
        self.probe_at = None;
        self.pace_at = None;
    }

    fn clear_unacked(&mut self) {
        self.unacked.clear();
        self.in_flight = 0;
        self.resend_count = 0;
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

pub(crate) struct Shared {
    pub(crate) state: Mutex<ConnState>,
    pub(crate) nudge: Notify,
}

pub(crate) enum Role {
    Initiator(oneshot::Sender<io::Result<()>>),
    Responder,
}

#[derive(Clone, Copy)]
pub(crate) enum RoleKind {
    Initiator,
    Responder,
}

pub(crate) struct DriverConfig {
    pub transport: DatagramTransport,
    pub remote: SocketAddr,
    pub incoming: mpsc::Receiver<(UtpHeader, Bytes)>,
    pub registry: ConnRegistry,
    pub key: ConnKey,
    pub token: ConnectionToken,
    pub proxy_registry: Option<ProxyConnRegistry>,
}

#[derive(Clone)]
pub(crate) enum DatagramTransport {
    Direct(Arc<UdpSender>),
    Proxy(Arc<ProxyAssociation>),
}

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
            seq_nr: match kind {
                RoleKind::Initiator => 2,
                RoleKind::Responder => rand::random::<u16>() | 1,
            },
            ack_nr: 0,
            send_buf: VecDeque::new(),
            unacked: VecDeque::new(),
            in_flight: 0,
            resend_count: 0,
            recv_ready: VecDeque::new(),
            reorder: BTreeMap::new(),
            reorder_bytes: 0,
            peer_wnd: RECV_BUF_MAX as u32,
            max_window: INITIAL_CWND,
            slow_start: true,
            ss_thresh: usize::MAX,
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
            last_maxed_out: None,
            pace_at: None,
            fin_acked_at: None,
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

pub(crate) async fn drive(shared: Arc<Shared>, mut cfg: DriverConfig, role: Role) {
    {
        let mut st = shared.state.lock();
        st.pmtud =
            matches!(&cfg.transport, DatagramTransport::Direct(sender) if sender.can_probe());
        if let Role::Initiator(tx) = role {
            st.connect_notify = Some(tx);
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
            let state_bytes = st.encode_state();
            st.outbox.push(state_bytes);
        }
    }
    flush(&shared, &cfg).await;

    let mut closed_since: Option<Instant> = None;
    let mut sleep = Box::pin(tokio::time::sleep(Duration::from_secs(3600)));
    let mut armed: Option<Instant> = None;
    loop {
        let deadline = {
            let st = shared.state.lock();
            if st.state == State::Closed && closed_since.is_none() {
                closed_since = Some(Instant::now());
            }
            st.next_deadline()
        };

        if let Some(since) = closed_since {
            let drained = {
                let st = shared.state.lock();
                st.unacked.is_empty() && st.send_buf.is_empty()
            };
            if drained || since.elapsed() > LINGER_TIMEOUT {
                break;
            }
        }

        let stale = match (armed, deadline) {
            (None, None) => false,
            (Some(a), Some(d)) => a.max(d) - a.min(d) > Duration::from_millis(1),
            _ => true,
        };
        if stale {
            if let Some(d) = deadline {
                sleep.as_mut().reset(
                    tokio::time::Instant::now() + d.saturating_duration_since(Instant::now()),
                );
            }
            armed = deadline;
        }

        tokio::select! {
            pkt = cfg.incoming.recv() => {
                match pkt {
                    Some((header, payload)) => {
                        let mut st = shared.state.lock();
                        st.handle_packet(&header, &payload);
                        for _ in 0..ACK_BATCH {
                            let Ok((header, payload)) = cfg.incoming.try_recv() else {
                                break;
                            };
                            st.handle_packet(&header, &payload);
                        }
                    }
                    None => {
                        shared.state.lock().fail(io::ErrorKind::ConnectionAborted);
                    }
                }
            }
            _ = shared.nudge.notified() => {
                shared.state.lock().note_window_update();
            }
            _ = &mut sleep, if armed.is_some() => {
                armed = None;
                shared.state.lock().check_timers();
            }
        }

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

async fn flush(shared: &Arc<Shared>, cfg: &DriverConfig) {
    let probe = {
        let mut st = shared.state.lock();
        if st
            .send_retry_at
            .is_some_and(|retry_at| retry_at > Instant::now())
        {
            return;
        }
        st.send_retry_at = None;
        st.probe_out.take()
    };
    if let Some((seq, bytes)) = probe {
        let sent = match &cfg.transport {
            DatagramTransport::Direct(sender) => sender.send_probe(&bytes, cfg.remote).await,
            DatagramTransport::Proxy(_) => Err(io::Error::from(io::ErrorKind::Unsupported)),
        };
        if let Err(error) = sent {
            let too_big = is_message_too_big(&error);
            if !too_big {
                tracing::debug!("µTP MTU probe to {} failed: {error}", cfg.remote);
            }
            shared.state.lock().mtu_probe_send_failed(seq, too_big);
        }
    }
    let datagrams = std::mem::take(&mut shared.state.lock().outbox);
    let mut datagrams = datagrams.into_iter();
    while let Some(d) = datagrams.next() {
        let result = match &cfg.transport {
            DatagramTransport::Direct(sender) => sender.send_to(&d, cfg.remote).await,
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

const NO_BUFFER_CODES: &[i32] = if cfg!(windows) {
    &[10055]
} else if cfg!(any(target_os = "linux", target_os = "android")) {
    &[105, 12]
} else {
    &[55, 12]
};

fn is_recoverable_send_error(error: &io::Error) -> bool {
    if error
        .raw_os_error()
        .is_some_and(|code| NO_BUFFER_CODES.contains(&code))
    {
        return true;
    }
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
            let was_owed = st.needs_ack;
            st.note_window_update();
            let wake = st.needs_ack && !was_owed;
            drop(st);
            if wake {
                self.shared.nudge.notify_one();
            }
            return Poll::Ready(Ok(()));
        }
        if let Some(kind) = st.error {
            return Poll::Ready(Err(io::Error::from(kind)));
        }
        if st.eof {
            return Poll::Ready(Ok(()));
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
        st.handle_packet(&ack(1, Some(vec![0b0000_0011, 0, 0, 0])), &[]);
        assert!(st.outbox.is_empty());
        assert_eq!(st.unacked.len(), 4);
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
        assert_eq!(st.max_window, st.packet_size);
        assert!(st.slow_start, "a timeout re-enters slow start");
        assert!(st.ss_thresh >= 2 * st.packet_size);
        assert_eq!(st.rto, INITIAL_RTO * 2);
        assert_eq!(st.outbox.len(), 1);
        assert_eq!(st.bytes_in_flight(), MSS);
        assert!(st.unacked.iter().skip(1).all(|p| p.need_resend));
        st.outbox.clear();
        st.fill_send_window();
        assert!(st.outbox.is_empty(), "a one-packet window holds one packet");
        st.handle_packet(&ack(2, None), &[]);
        st.fill_send_window();
        assert_eq!(st.outbox.len(), 2);
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
        st.send_buf.extend([1u8; 10]);
        st.fill_send_window();
        assert_eq!(st.outbox.len(), 1);
        st.outbox.clear();
        st.send_buf.extend([2u8; 10]);
        st.fill_send_window();
        st.send_buf.extend([3u8; 10]);
        st.fill_send_window();
        assert!(st.outbox.is_empty());
        st.handle_packet(&ack(2, None), &[]);
        st.fill_send_window();
        assert_eq!(st.outbox.len(), 1);
        let (_, payload) = UtpHeader::decode(&st.outbox[0]).unwrap();
        assert_eq!(payload.len(), 20);
        st.outbox.clear();
        st.send_buf.extend(std::iter::repeat_n(4u8, MSS + 5));
        st.fill_send_window();
        assert_eq!(st.outbox.len(), 1);
        st.want_fin = true;
        st.fill_send_window();
        assert_eq!(st.outbox.len(), 2);
    }

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
        assert_eq!(probe_bytes.len(), (floor + ceiling) / 2);
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
        st.mtu_probe_send_failed(seq, true);
        assert_eq!(st.mtu_ceiling, bytes.len() - 1);
        assert_eq!(st.outbox.len(), 1);
        assert_eq!(UtpHeader::decode(&st.outbox[0]).unwrap().0.seq_nr, seq);
    }

    #[test]
    fn probe_send_error_other_than_too_big_keeps_the_ceiling() {
        let shared = probing_initiator();
        let mut st = shared.state.lock();
        let ceiling = st.mtu_ceiling;
        st.send_buf.extend(std::iter::repeat_n(9u8, 8 * MSS));
        st.fill_send_window();
        let (seq, _) = st.probe_out.take().unwrap();
        st.outbox.clear();
        st.mtu_probe_send_failed(seq, false);
        assert_eq!(st.mtu_ceiling, ceiling);
        assert!(st.mtu_probe.is_none(), "an unprobed resend proves nothing");
        assert_eq!(st.outbox.len(), 1);
        assert_eq!(UtpHeader::decode(&st.outbox[0]).unwrap().0.seq_nr, seq);
    }

    #[test]
    fn acked_probes_close_the_search() {
        let shared = probing_initiator();
        let mut st = shared.state.lock();
        let ceiling = st.mtu_ceiling;
        for _ in 0..16 {
            st.send_buf.extend(std::iter::repeat_n(9u8, 8 * MSS));
            st.fill_send_window();
            let Some((_, bytes)) = st.probe_out.take() else {
                break;
            };
            let floor = st.mtu_floor;
            assert_eq!(bytes.len(), (floor + st.mtu_ceiling) / 2);
            let last = st.seq_nr.wrapping_sub(1);
            st.handle_packet(&ack(last, None), &[]);
            assert!(st.mtu_floor > floor, "every acked probe raises the floor");
            st.send_buf.clear();
            st.outbox.clear();
        }
        assert_eq!(st.mtu_ceiling, ceiling);
        assert!(st.mtu_floor + MTU_SEARCH_GRANULARITY > ceiling);
    }

    #[test]
    fn losing_a_probe_no_bigger_than_the_floor_is_ordinary_loss() {
        let shared = probing_initiator();
        let mut st = shared.state.lock();
        let (floor, ceiling) = (st.mtu_floor, st.mtu_ceiling);
        st.mtu_probe = Some((7, floor));
        assert!(!st.on_mtu_probe_lost(7));
        assert_eq!((st.mtu_floor, st.mtu_ceiling), (floor, ceiling));
        assert!(st.mtu_probe.is_none());
    }

    #[test]
    fn mtu_search_reopens_once_data_follows_the_reprobe_interval() {
        let shared = probing_initiator();
        let mut st = shared.state.lock();
        st.mtu_ceiling = st.mtu_floor;
        st.mtu_reprobe_at = Instant::now() - Duration::from_millis(1);
        st.send_buf.extend(std::iter::repeat_n(9u8, 8 * MSS));
        st.fill_send_window();
        assert_eq!(st.mtu_ceiling, MAX_DATAGRAM_V4);
        assert!(st.probe_out.is_some());
        assert!(st.mtu_reprobe_at > Instant::now());
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
        st.reorder.insert(102, vec![1]);
        st.reorder.insert(172, vec![1]);
        let mask = st.build_selective_ack().unwrap();
        assert_eq!(mask.len(), 12);
        assert_eq!(mask[0] & 1, 1);
        assert_ne!(mask[70 / 8] & (1 << (70 % 8)), 0);
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
        assert_eq!(
            hist.add_sample(51_000, t0),
            0,
            "min of recent samples still 1 ms"
        );
        hist.add_sample(51_000, t0);
        assert_eq!(hist.add_sample(51_000, t0), 50_000);
        assert_eq!(hist.add_sample(20_000, t0 + BASE_DELAY_BUCKET), 19_000);
        assert_eq!(hist.add_sample(20_000, t0 + BASE_DELAY_BUCKET * 2), 0);
        assert!(wrapping_lt(u32::MAX - 5, 3));
    }

    #[test]
    fn ledbat_grows_below_target_and_shrinks_above_it() {
        let shared = connected_initiator();
        let mut st = shared.state.lock();
        let window = st.max_window;
        st.last_maxed_out = Some(Instant::now());
        st.update_cwnd(1_000, window);
        assert!(st.max_window > window && st.max_window <= window + 3000);
        let grown = st.max_window;
        st.update_cwnd(400_000, grown);
        assert!(st.max_window >= grown, "one high sample is filtered");
        for _ in 0..CUR_DELAY_SAMPLES {
            st.update_cwnd(400_000, grown);
        }
        assert!(st.max_window < grown);
        let current = st.max_window;
        st.last_maxed_out = None;
        st.update_cwnd(1_000, MSS);
        assert_eq!(st.max_window, current);
    }

    fn assert_counters(st: &ConnState) {
        let flight: usize = st
            .unacked
            .iter()
            .filter(|p| !p.need_resend)
            .map(|p| p.payload.len())
            .sum();
        let resend = st.unacked.iter().filter(|p| p.need_resend).count();
        assert_eq!(st.in_flight, flight);
        assert_eq!(st.resend_count, resend);
        let reorder: usize = st.reorder.values().map(Vec::len).sum();
        assert_eq!(st.reorder_bytes, reorder);
    }

    #[test]
    fn in_flight_counters_track_acks_sacks_and_timeouts() {
        let shared = connected_initiator();
        let mut st = shared.state.lock();
        send_packets(&mut st, 40);
        assert_counters(&st);
        let mut mask = vec![0xFFu8; 8];
        mask[0] &= !1;
        st.handle_packet(&ack(1, Some(mask)), &[]);
        assert_counters(&st);
        let resent: Vec<u16> = st
            .outbox
            .iter()
            .map(|d| UtpHeader::decode(d).unwrap().0.seq_nr)
            .collect();
        assert_eq!(resent, vec![2, 3], "only the two holes are resent");
        assert!(st.unacked.len() <= 3);
        st.rto_base = Some(Instant::now() - Duration::from_secs(5));
        st.check_timers();
        assert_counters(&st);
        st.handle_packet(&ack(41, None), &[]);
        assert_counters(&st);
        assert_eq!(st.in_flight, 0);
    }

    #[test]
    fn out_of_order_receive_tracks_buffered_bytes() {
        let shared = connected_initiator();
        let mut st = shared.state.lock();
        let data = |seq: u16| {
            let mut h = ack(1, None);
            h.packet_type = PacketType::Data;
            h.seq_nr = seq;
            h
        };
        st.handle_packet(&data(103), b"cc");
        st.handle_packet(&data(102), b"bbb");
        st.handle_packet(&data(102), b"bbb");
        assert_eq!(st.reorder_bytes, 5);
        assert_counters(&st);
        st.handle_packet(&data(101), b"a");
        assert_eq!(st.reorder_bytes, 0);
        assert_eq!(st.recv_ready.len(), 6);
        assert_eq!(st.advertised_window() as usize, RECV_BUF_MAX - 6);
    }

    #[test]
    fn window_growth_survives_ack_batching() {
        let shared = connected_initiator();
        let mut st = shared.state.lock();
        st.slow_start = true;
        st.max_window = 4 * MSS;
        send_packets(&mut st, 4);
        st.send_buf.extend(std::iter::repeat_n(7u8, 4 * MSS));
        st.fill_send_window();
        assert!(st.last_maxed_out.is_some());
        let before = st.max_window;
        for seq in 2..=5 {
            st.handle_packet(&ack(seq, None), &[]);
        }
        assert!(st.max_window >= before + 4 * MSS, "{}", st.max_window);
    }

    #[test]
    fn unacked_fin_acker_without_a_fin_times_out() {
        let shared = connected_initiator();
        let mut st = shared.state.lock();
        st.want_fin = true;
        st.maybe_send_fin();
        st.handle_packet(&ack(2, None), &[]);
        assert_eq!(st.state, State::FinSent);
        assert!(st.fin_acked_at.is_some());
        assert!(st.next_deadline().is_some());
        st.check_timers();
        assert_eq!(st.state, State::FinSent);
        st.fin_acked_at = Some(Instant::now() - LINGER_TIMEOUT - Duration::from_secs(1));
        st.check_timers();
        assert_eq!(st.state, State::Closed);
        assert!(st.eof);
    }

    #[test]
    fn buffer_exhaustion_is_retriable() {
        for code in NO_BUFFER_CODES {
            assert!(is_recoverable_send_error(&io::Error::from_raw_os_error(
                *code
            )));
        }
    }

    #[test]
    fn large_windows_are_sent_in_paced_bursts() {
        let shared = connected_initiator();
        let mut st = shared.state.lock();
        st.rtt = 0.1;
        st.max_window = 800 * MSS;
        st.send_buf.extend(std::iter::repeat_n(7u8, 100 * MSS));
        st.fill_send_window();
        let burst = st.burst_limit();
        assert_eq!(burst, MIN_BURST_PACKETS);
        assert_eq!(st.unacked.len(), burst);
        assert!(st.pace_at.is_some() && st.next_deadline().is_some());
        st.fill_send_window();
        assert_eq!(st.unacked.len(), burst, "held until the pace timer fires");
        st.pace_at = Some(Instant::now() - Duration::from_millis(1));
        st.fill_send_window();
        assert_eq!(st.unacked.len(), 2 * burst);
        assert_counters(&st);
        st.rtt = 0.0001;
        assert!(st.burst_limit() > 800);
    }

    #[test]
    fn a_lost_fast_retransmit_is_repaired_after_an_rto() {
        let shared = connected_initiator();
        let mut st = shared.state.lock();
        send_packets(&mut st, 6);
        let sack = Some(vec![0b0000_0111, 0, 0, 0]);
        st.handle_packet(&ack(1, sack.clone()), &[]);
        assert_eq!(st.outbox.len(), 1);
        st.outbox.clear();
        st.handle_packet(&ack(1, sack.clone()), &[]);
        assert!(st.outbox.is_empty(), "not resent again straight away");
        st.unacked[0].sent_at = Instant::now() - st.rto - Duration::from_millis(1);
        st.handle_packet(&ack(1, sack), &[]);
        assert_eq!(st.outbox.len(), 1);
    }
}
