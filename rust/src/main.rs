//! bridgekeeper (Rust, std-only) -- resident WSL daemon fronting the
//! two host NPU services: Lemonade (127.0.0.1:13305, /api/v1) and
//! FastFlowLM (127.0.0.1:53999, /v1). Contract: ../SPEC.md.
//!
//! Design in one paragraph: a TcpListener on the main thread, one OS
//! thread per connection, one polling thread for upstream health.
//! Every handler runs inside catch_unwind, so a panicking handler
//! kills at most its own connection -- never the listener. All
//! upstream calls are blocking sockets with explicit connect/read
//! timeouts, which is what makes the contract's fail-fast rules
//! (502 within 2 s, SSE error event instead of a hang) mechanical
//! rather than hopeful. No dependencies: the HTTP and JSON surface
//! of this contract is small enough to implement directly, and the
//! scorecard counts deps, build time, binary size and startup time.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::panic;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ------------------------------------------------------------------
// Contract constants (SPEC: no config files, no CLI flags)
// ------------------------------------------------------------------

const BIND_ADDR: &str = "0.0.0.0:14112";
const LEMOND: &str = "127.0.0.1:13305";
const FLM: &str = "127.0.0.1:53999";
const CHAT_MODEL: &str = "llama3.2:1b";
const EMBED_MODEL: &str = "nomic-embed-text-v1-GGUF";
const STT_MODEL: &str = "Whisper-Large-v3-Turbo";
const POLL_INTERVAL: Duration = Duration::from_secs(2);
const MAX_BODY: u64 = 256 * 1024 * 1024; // request bodies (WAV uploads)

// Timeouts. Refused connections on loopback fail instantly; these
// guard the "accepted but wedged" cases.
const POLL_CONNECT: Duration = Duration::from_millis(700);
const POLL_READ: Duration = Duration::from_secs(1);
const EMBED_CONNECT: Duration = Duration::from_secs(1);
const EMBED_READ: Duration = Duration::from_secs(30);
const CHAT_CONNECT: Duration = Duration::from_secs(2);
const CHAT_IDLE: Duration = Duration::from_secs(120); // per-read idle timeout
const STT_CONNECT: Duration = Duration::from_secs(2);
const STT_READ: Duration = Duration::from_secs(900); // cold model loads are slow
const CLIENT_READ: Duration = Duration::from_secs(300);

// ------------------------------------------------------------------
// Shared state
// ------------------------------------------------------------------

struct State {
    started: Instant,
    lemond_up: AtomicBool,
    flm_up: AtomicBool,
    requests: AtomicU64,
    errors: AtomicU64,
    panics: AtomicU64,
}

impl State {
    fn new() -> Self {
        State {
            started: Instant::now(),
            lemond_up: AtomicBool::new(false),
            flm_up: AtomicBool::new(false),
            requests: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            panics: AtomicU64::new(0),
        }
    }
}

// Log-line uptime base (State carries its own base for /health).
static BOOT: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

fn log(msg: &str) {
    let t = BOOT.get().map(|b| b.elapsed().as_secs_f32()).unwrap_or(0.0);
    eprintln!("[bridgekeeper +{:>8.1}s] {}", t, msg);
}

// ------------------------------------------------------------------
// Minimal JSON helpers.
//
// We never build a DOM. We extract the few fields the contract needs
// and copy the embedding array through as raw text, so a 768-float
// response costs one substring copy and zero float parsing.
// ------------------------------------------------------------------

/// Escape a string for inclusion in a JSON string literal.
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// Parse a JSON string starting at the opening quote at `start`.
/// Returns the unescaped string and the index just past the closing
/// quote. Deliberately lenient: malformed escapes degrade to literal
/// characters instead of errors (a weird token from the model should
/// not kill a stream).
fn parse_json_string(bytes: &[u8], start: usize) -> Option<(String, usize)> {
    if bytes.get(start) != Some(&b'"') {
        return None;
    }
    let mut out: Vec<u8> = Vec::new();
    let mut i = start + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                return Some((String::from_utf8_lossy(&out).into_owned(), i + 1));
            }
            b'\\' => {
                i += 1;
                let e = *bytes.get(i)?;
                match e {
                    b'"' => out.push(b'"'),
                    b'\\' => out.push(b'\\'),
                    b'/' => out.push(b'/'),
                    b'b' => out.push(0x08),
                    b'f' => out.push(0x0c),
                    b'n' => out.push(b'\n'),
                    b'r' => out.push(b'\r'),
                    b't' => out.push(b'\t'),
                    b'u' => {
                        let hex = |at: usize| -> Option<u16> {
                            let s = std::str::from_utf8(bytes.get(at..at + 4)?).ok()?;
                            u16::from_str_radix(s, 16).ok()
                        };
                        let hi = hex(i + 1)?;
                        i += 4;
                        let cp: u32 = if (0xD800..0xDC00).contains(&(hi as u32)) {
                            // Possible surrogate pair: "\uXXXX\uYYYY".
                            if bytes.get(i + 1) == Some(&b'\\')
                                && bytes.get(i + 2) == Some(&b'u')
                                && (0xDC00..0xE000).contains(&(hex(i + 3)? as u32))
                            {
                                let lo = hex(i + 3)? as u32;
                                i += 6;
                                0x10000 + (((hi as u32 - 0xD800) << 10) | (lo - 0xDC00))
                            } else {
                                0xFFFD
                            }
                        } else {
                            hi as u32
                        };
                        let ch = char::from_u32(cp).unwrap_or('\u{FFFD}');
                        let mut buf = [0u8; 4];
                        out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                    }
                    other => out.push(other), // lenient: keep the char
                }
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    None // unterminated
}

/// Find `"key"` followed by `:` (searching from `from`); return the
/// index of the value's first byte.
fn find_key_value_start(json: &[u8], key: &str, from: usize) -> Option<usize> {
    let needle = format!("\"{}\"", key);
    let nb = needle.as_bytes();
    let mut i = from;
    while i + nb.len() <= json.len() {
        if &json[i..i + nb.len()] == nb {
            let mut j = i + nb.len();
            while j < json.len() && (json[j] as char).is_whitespace() {
                j += 1;
            }
            if json.get(j) == Some(&b':') {
                let mut k = j + 1;
                while k < json.len() && (json[k] as char).is_whitespace() {
                    k += 1;
                }
                return Some(k);
            }
            i = j; // matched the word but not as a key; keep looking
        } else {
            i += 1;
        }
    }
    None
}

/// Extract a string field by key, searching from byte offset `from`.
fn extract_string(json: &[u8], key: &str, from: usize) -> Option<String> {
    let v = find_key_value_start(json, key, from)?;
    parse_json_string(json, v).map(|(s, _)| s)
}

/// Extract an integer field by key.
fn extract_int(json: &[u8], key: &str) -> Option<i64> {
    let v = find_key_value_start(json, key, 0)?;
    let mut end = v;
    if json.get(end) == Some(&b'-') {
        end += 1;
    }
    let digits_start = end;
    while end < json.len() && json[end].is_ascii_digit() {
        end += 1;
    }
    if end == digits_start {
        return None;
    }
    std::str::from_utf8(&json[v..end]).ok()?.parse().ok()
}

// ------------------------------------------------------------------
// HTTP body framing: a chunked-transfer decoder generic over any
// buffered reader (upstream sockets AND downstream request bodies).
// ------------------------------------------------------------------

struct ChunkedReader<R: BufRead> {
    inner: R,
    chunk_left: u64,
    state: u8, // 0 = want size line, 1 = in chunk data, 2 = trailers, 3 = done
}

impl<R: BufRead> ChunkedReader<R> {
    fn new(inner: R) -> Self {
        ChunkedReader { inner, chunk_left: 0, state: 0 }
    }
}

impl<R: BufRead> Read for ChunkedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            match self.state {
                0 => {
                    let mut line = String::new();
                    let n = self.inner.read_line(&mut line)?;
                    if n == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "eof in chunk header",
                        ));
                    }
                    let size_str = line.trim().split(';').next().unwrap_or("");
                    if size_str.is_empty() {
                        continue; // tolerate a stray CRLF between chunks
                    }
                    let size = u64::from_str_radix(size_str, 16).map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "bad chunk size")
                    })?;
                    if size == 0 {
                        self.state = 2;
                    } else {
                        self.chunk_left = size;
                        self.state = 1;
                    }
                }
                1 => {
                    if self.chunk_left == 0 {
                        // Consume the CRLF terminating the chunk.
                        let mut crlf = [0u8; 2];
                        self.inner.read_exact(&mut crlf)?;
                        self.state = 0;
                        continue;
                    }
                    let want = std::cmp::min(buf.len() as u64, self.chunk_left) as usize;
                    let n = self.inner.read(&mut buf[..want])?;
                    if n == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "eof mid-chunk",
                        ));
                    }
                    self.chunk_left -= n as u64;
                    return Ok(n);
                }
                2 => {
                    let mut line = String::new();
                    let n = self.inner.read_line(&mut line)?;
                    if n == 0 || line.trim().is_empty() {
                        self.state = 3;
                        return Ok(0);
                    }
                }
                _ => return Ok(0),
            }
        }
    }
}

/// The framed body of an HTTP message, from its headers.
fn body_reader<R: BufRead + Send + 'static>(
    inner: R,
    headers: &[(String, String)],
) -> Box<dyn Read + Send> {
    let mut chunked = false;
    let mut length: Option<u64> = None;
    for (k, v) in headers {
        if k == "transfer-encoding" && v.to_lowercase().contains("chunked") {
            chunked = true;
        }
        if k == "content-length" {
            length = v.trim().parse().ok();
        }
    }
    if chunked {
        Box::new(ChunkedReader::new(inner))
    } else if let Some(n) = length {
        Box::new(inner.take(n))
    } else {
        Box::new(inner) // read to EOF (peers get Connection: close)
    }
}

fn parse_headers<R: BufRead>(reader: &mut R) -> io::Result<Vec<(String, String)>> {
    let mut headers = Vec::new();
    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line)?;
        if n == 0 || line.trim().is_empty() {
            break;
        }
        if headers.len() > 200 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "too many headers"));
        }
        if let Some((k, v)) = line.split_once(':') {
            headers.push((k.trim().to_lowercase(), v.trim().to_string()));
        }
    }
    Ok(headers)
}

// ------------------------------------------------------------------
// Upstream HTTP client (blocking, std::net)
// ------------------------------------------------------------------

struct Upstream {
    status: u16,
    body: Box<dyn Read + Send>,
}

/// One blocking HTTP request to a loopback upstream.
/// Returns status + framed body reader. Any io failure is Err --
/// callers translate that into the contract's error shape.
fn upstream_request(
    addr: &str,
    method: &str,
    path: &str,
    content_type: Option<&str>,
    body: &[u8],
    connect_timeout: Duration,
    read_timeout: Duration,
) -> io::Result<Upstream> {
    let sock_addr: SocketAddr =
        addr.parse().map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let stream = TcpStream::connect_timeout(&sock_addr, connect_timeout)?;
    stream.set_read_timeout(Some(read_timeout))?;
    stream.set_write_timeout(Some(read_timeout))?;
    stream.set_nodelay(true).ok();
    let mut writer = stream.try_clone()?;
    let mut head = format!(
        "{} {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Length: {}\r\n",
        method,
        path,
        addr,
        body.len()
    );
    if let Some(ct) = content_type {
        head.push_str(&format!("Content-Type: {}\r\n", ct));
    }
    head.push_str("\r\n");
    writer.write_all(head.as_bytes())?;
    if !body.is_empty() {
        writer.write_all(body)?;
    }
    writer.flush()?;

    let mut reader = BufReader::with_capacity(16 * 1024, stream);
    let mut status_line = String::new();
    if reader.read_line(&mut status_line)? == 0 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "no response"));
    }
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad status line"))?;
    let headers = parse_headers(&mut reader)?;
    Ok(Upstream { status, body: body_reader(reader, &headers) })
}

/// Health probe: GET a model-list endpoint, 200 means up.
fn probe(addr: &str, path: &str) -> bool {
    match upstream_request(addr, "GET", path, None, &[], POLL_CONNECT, POLL_READ) {
        Ok(up) => up.status == 200,
        Err(_) => false,
    }
}

/// Poll both upstreams every ~2 s, forever.
fn poll_loop(state: Arc<State>) {
    loop {
        state.lemond_up.store(probe(LEMOND, "/api/v1/models"), Ordering::Relaxed);
        state.flm_up.store(probe(FLM, "/v1/models"), Ordering::Relaxed);
        std::thread::sleep(POLL_INTERVAL);
    }
}

// ------------------------------------------------------------------
// Downstream (client-facing) plumbing
// ------------------------------------------------------------------

struct Request {
    method: String,
    path: String,
    body: Vec<u8>,
}

/// Read and parse one HTTP request from the client socket.
fn read_request(reader: &mut BufReader<TcpStream>) -> io::Result<Option<Request>> {
    let mut request_line = String::new();
    if reader.read_line(&mut request_line)? == 0 {
        return Ok(None); // client went away
    }
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let raw_path = parts.next().unwrap_or("").to_string();
    if method.is_empty() || raw_path.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad request line"));
    }
    let path = raw_path.split('?').next().unwrap_or("").to_string();
    let headers = parse_headers(reader)?;
    let mut chunked = false;
    let mut length: u64 = 0;
    for (k, v) in &headers {
        if k == "transfer-encoding" && v.to_lowercase().contains("chunked") {
            chunked = true;
        }
        if k == "content-length" {
            length = v.trim().parse().unwrap_or(0);
        }
    }
    let mut body = Vec::new();
    if chunked {
        // &mut BufReader implements BufRead + Read, so the same
        // decoder used for upstream responses works here, with no
        // buffering hazards: it reads through the shared buffer.
        let mut dec = ChunkedReader::new(reader.by_ref());
        dec.by_ref().take(MAX_BODY + 1).read_to_end(&mut body)?;
        if body.len() as u64 > MAX_BODY {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "body too large"));
        }
    } else {
        if length > MAX_BODY {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "body too large"));
        }
        reader.by_ref().take(length).read_to_end(&mut body)?;
    }
    Ok(Some(Request { method, path, body }))
}

fn write_json(writer: &mut TcpStream, status: u16, body: &str) -> io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        413 => "Payload Too Large",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        _ => "Error",
    };
    let head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        status,
        reason,
        body.len()
    );
    writer.write_all(head.as_bytes())?;
    writer.write_all(body.as_bytes())?;
    writer.flush()
}

fn err_json(msg: &str) -> String {
    format!("{{\"error\":\"{}\"}}", json_escape(msg))
}

// ------------------------------------------------------------------
// Route handlers. Each runs on its own connection thread inside
// catch_unwind (see main). Contract failures increment errors;
// transport failures to the *client* just end the handler.
// ------------------------------------------------------------------

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn handle_health(writer: &mut TcpStream, state: &State) -> io::Result<()> {
    let up = |b: bool| if b { "up" } else { "down" };
    let body = format!(
        "{{\"ok\":true,\"uptime_s\":{},\"lemond\":\"{}\",\"flm\":\"{}\",\"requests\":{},\"errors\":{}}}",
        state.started.elapsed().as_secs(),
        up(state.lemond_up.load(Ordering::Relaxed)),
        up(state.flm_up.load(Ordering::Relaxed)),
        state.requests.load(Ordering::Relaxed),
        state.errors.load(Ordering::Relaxed),
    );
    write_json(writer, 200, &body)
}

// --- /chat: SSE proxy to FastFlowLM --------------------------------

fn sse_headers(writer: &mut TcpStream) -> io::Result<()> {
    writer.write_all(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
    )?;
    writer.flush()
}

/// Send one SSE event (`data: <json>\n\n`) as one chunk, flushed --
/// tokens must reach the client promptly, never batched.
fn sse_send(writer: &mut TcpStream, json_payload: &str) -> io::Result<()> {
    let event = format!("data: {}\n\n", json_payload);
    let frame = format!("{:x}\r\n{}\r\n", event.len(), event);
    writer.write_all(frame.as_bytes())?;
    writer.flush()
}

fn sse_close(writer: &mut TcpStream) -> io::Result<()> {
    writer.write_all(b"0\r\n\r\n")?;
    writer.flush()
}

fn handle_chat(writer: &mut TcpStream, state: &State, req: &Request) -> io::Result<()> {
    let prompt = match extract_string(&req.body, "prompt", 0) {
        Some(p) => p,
        None => {
            state.errors.fetch_add(1, Ordering::Relaxed);
            return write_json(writer, 400, &err_json("missing \"prompt\" in request body"));
        }
    };
    let max_tokens = extract_int(&req.body, "max_tokens").unwrap_or(512).clamp(1, 16384);
    let payload = format!(
        "{{\"model\":\"{}\",\"messages\":[{{\"role\":\"user\",\"content\":\"{}\"}}],\"max_tokens\":{},\"stream\":true}}",
        CHAT_MODEL,
        json_escape(&prompt),
        max_tokens
    );

    sse_headers(writer)?;
    // Past this point the response is committed to SSE: every failure
    // is reported as an error event on the stream, then close. The
    // stream must never hang and never take the daemon down.
    let fail = |writer: &mut TcpStream, state: &State, msg: String| -> io::Result<()> {
        state.errors.fetch_add(1, Ordering::Relaxed);
        log(&format!("chat: {}", msg));
        let _ = sse_send(writer, &format!("{{\"error\":\"{}\"}}", json_escape(&msg)));
        let _ = sse_close(writer);
        Ok(())
    };

    let up = match upstream_request(
        FLM,
        "POST",
        "/v1/chat/completions",
        Some("application/json"),
        payload.as_bytes(),
        CHAT_CONNECT,
        CHAT_IDLE,
    ) {
        Ok(u) => u,
        Err(e) => return fail(writer, state, format!("flm unreachable: {}", e)),
    };
    if up.status != 200 {
        let mut body = up.body;
        let mut buf = Vec::new();
        let _ = body.by_ref().take(2048).read_to_end(&mut buf);
        return fail(writer, state, format!("flm returned HTTP {}", up.status));
    }

    let mut lines = BufReader::with_capacity(8192, up.body);
    let mut saw_finish = false;
    loop {
        let mut line: Vec<u8> = Vec::new();
        match lines.read_until(b'\n', &mut line) {
            Ok(0) => {
                // Upstream closed the connection.
                if saw_finish {
                    let _ = sse_send(writer, "{\"done\":true}");
                    let _ = sse_close(writer);
                    return Ok(());
                }
                return fail(writer, state, "upstream closed the stream early".to_string());
            }
            Ok(_) => {
                let t = trim_ascii(&line);
                if !t.starts_with(b"data:") {
                    continue;
                }
                let data = trim_ascii(&t[5..]);
                if data == b"[DONE]" {
                    let _ = sse_send(writer, "{\"done\":true}");
                    let _ = sse_close(writer);
                    return Ok(());
                }
                if let Some(fr) = find_key_value_start(data, "finish_reason", 0) {
                    if data.get(fr) == Some(&b'"') {
                        saw_finish = true;
                    }
                }
                if let Some(dpos) = find_subslice(data, b"\"delta\"") {
                    if let Some(delta) = extract_string(data, "content", dpos) {
                        if !delta.is_empty()
                            && sse_send(
                                writer,
                                &format!("{{\"delta\":\"{}\"}}", json_escape(&delta)),
                            )
                            .is_err()
                        {
                            // Client went away mid-stream. Dropping
                            // `lines` closes the upstream socket, so
                            // FLM stops generating for a dead client.
                            return Ok(());
                        }
                    }
                }
            }
            Err(e) => {
                // Includes the idle-timeout case: an upstream that
                // stalls forever surfaces here as an error event.
                return fail(writer, state, format!("upstream stream failed: {}", e));
            }
        }
    }
}

fn trim_ascii(b: &[u8]) -> &[u8] {
    let mut s = 0;
    let mut e = b.len();
    while s < e && (b[s] as char).is_whitespace() {
        s += 1;
    }
    while e > s && (b[e - 1] as char).is_whitespace() {
        e -= 1;
    }
    &b[s..e]
}

// --- /embed: proxy to Lemonade embeddings ---------------------------

fn handle_embed(writer: &mut TcpStream, state: &State, req: &Request) -> io::Result<()> {
    let text = match extract_string(&req.body, "text", 0) {
        Some(t) => t,
        None => {
            state.errors.fetch_add(1, Ordering::Relaxed);
            return write_json(writer, 400, &err_json("missing \"text\" in request body"));
        }
    };
    let payload = format!(
        "{{\"model\":\"{}\",\"input\":\"{}\"}}",
        EMBED_MODEL,
        json_escape(&text)
    );
    let bad_gateway = |writer: &mut TcpStream, state: &State, msg: String| -> io::Result<()> {
        state.errors.fetch_add(1, Ordering::Relaxed);
        log(&format!("embed: {}", msg));
        write_json(writer, 502, &err_json(&msg))
    };
    let mut up = match upstream_request(
        LEMOND,
        "POST",
        "/api/v1/embeddings",
        Some("application/json"),
        payload.as_bytes(),
        EMBED_CONNECT,
        EMBED_READ,
    ) {
        Ok(u) => u,
        // Connection refused on loopback returns instantly, so a
        // dead lemond is a 502 in milliseconds -- far inside the
        // contract's 2 s bound.
        Err(e) => return bad_gateway(writer, state, format!("lemond unreachable: {}", e)),
    };
    if up.status != 200 {
        return bad_gateway(writer, state, format!("lemond embeddings returned HTTP {}", up.status));
    }
    let mut raw = Vec::new();
    if let Err(e) = up.body.by_ref().take(64 * 1024 * 1024).read_to_end(&mut raw) {
        return bad_gateway(writer, state, format!("lemond embeddings read failed: {}", e));
    }
    // Copy the embedding array through as raw text; count elements.
    let arr_start = match find_key_value_start(&raw, "embedding", 0) {
        Some(v) if raw.get(v) == Some(&b'[') => v,
        _ => {
            return bad_gateway(
                writer,
                state,
                "lemond embeddings response had no embedding array".to_string(),
            )
        }
    };
    let mut depth = 0i32;
    let mut arr_end: Option<usize> = None;
    for i in arr_start..raw.len() {
        match raw[i] {
            b'[' => depth += 1,
            b']' => {
                depth -= 1;
                if depth == 0 {
                    arr_end = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }
    let arr_end = match arr_end {
        Some(e) => e,
        None => {
            return bad_gateway(writer, state, "lemond embeddings array unterminated".to_string())
        }
    };
    let inner = trim_ascii(&raw[arr_start + 1..arr_end]);
    let valid = inner
        .iter()
        .all(|&b| b.is_ascii_digit() || b"eE+-., \t\r\n".contains(&b));
    if !valid {
        return bad_gateway(writer, state, "lemond embeddings array malformed".to_string());
    }
    let dims = if inner.is_empty() {
        0
    } else {
        inner.iter().filter(|&&b| b == b',').count() + 1
    };
    let array_text = String::from_utf8_lossy(&raw[arr_start..=arr_end]).into_owned();
    let body = format!("{{\"dims\":{},\"embedding\":{}}}", dims, array_text);
    write_json(writer, 200, &body)
}

// --- /transcribe: raw WAV in, Lemonade Whisper out ------------------

static BOUNDARY_SEQ: AtomicU64 = AtomicU64::new(0);

fn handle_transcribe(writer: &mut TcpStream, state: &State, req: &Request) -> io::Result<()> {
    if req.body.is_empty() {
        state.errors.fetch_add(1, Ordering::Relaxed);
        return write_json(writer, 400, &err_json("empty body: expected WAV bytes"));
    }
    let seq = BOUNDARY_SEQ.fetch_add(1, Ordering::Relaxed);
    let boundary = format!("----bridgekeeper-{}-{}", std::process::id(), seq);
    // Multipart shape mirrors the proven Python client: model field
    // first, then the file part.
    let mut mp: Vec<u8> = Vec::with_capacity(req.body.len() + 512);
    mp.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
    mp.extend_from_slice(b"Content-Disposition: form-data; name=\"model\"\r\n\r\n");
    mp.extend_from_slice(STT_MODEL.as_bytes());
    mp.extend_from_slice(b"\r\n");
    mp.extend_from_slice(format!("--{}\r\n", boundary).as_bytes());
    mp.extend_from_slice(
        b"Content-Disposition: form-data; name=\"file\"; filename=\"audio.wav\"\r\n",
    );
    mp.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
    mp.extend_from_slice(&req.body);
    mp.extend_from_slice(b"\r\n");
    mp.extend_from_slice(format!("--{}--\r\n", boundary).as_bytes());

    let bad_gateway = |writer: &mut TcpStream, state: &State, msg: String| -> io::Result<()> {
        state.errors.fetch_add(1, Ordering::Relaxed);
        log(&format!("transcribe: {}", msg));
        write_json(writer, 502, &err_json(&msg))
    };
    let ct = format!("multipart/form-data; boundary={}", boundary);
    let mut up = match upstream_request(
        LEMOND,
        "POST",
        "/api/v1/audio/transcriptions",
        Some(&ct),
        &mp,
        STT_CONNECT,
        STT_READ,
    ) {
        Ok(u) => u,
        Err(e) => return bad_gateway(writer, state, format!("lemond unreachable: {}", e)),
    };
    if up.status != 200 {
        return bad_gateway(
            writer,
            state,
            format!("lemond transcriptions returned HTTP {}", up.status),
        );
    }
    let mut raw = Vec::new();
    if let Err(e) = up.body.by_ref().take(16 * 1024 * 1024).read_to_end(&mut raw) {
        return bad_gateway(writer, state, format!("lemond transcriptions read failed: {}", e));
    }
    match extract_string(&raw, "text", 0) {
        Some(text) => {
            let body = format!("{{\"text\":\"{}\"}}", json_escape(&text));
            write_json(writer, 200, &body)
        }
        None => bad_gateway(
            writer,
            state,
            "lemond transcriptions response had no text field".to_string(),
        ),
    }
}

// ------------------------------------------------------------------
// Connection dispatch + listener
// ------------------------------------------------------------------

fn handle_conn(stream: TcpStream, state: Arc<State>) {
    let _ = stream.set_nodelay(true);
    let _ = stream.set_read_timeout(Some(CLIENT_READ));
    let read_half = match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };
    let mut reader = BufReader::with_capacity(16 * 1024, read_half);
    let mut writer = stream;
    let req = match read_request(&mut reader) {
        Ok(Some(r)) => r,
        Ok(None) => return, // client connected and left silently
        Err(e) => {
            state.errors.fetch_add(1, Ordering::Relaxed);
            let _ = write_json(&mut writer, 400, &err_json(&format!("bad request: {}", e)));
            return;
        }
    };
    state.requests.fetch_add(1, Ordering::Relaxed);
    let result = match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/health") => handle_health(&mut writer, &state),
        ("POST", "/chat") => handle_chat(&mut writer, &state, &req),
        ("POST", "/embed") => handle_embed(&mut writer, &state, &req),
        ("POST", "/transcribe") => handle_transcribe(&mut writer, &state, &req),
        _ => {
            state.errors.fetch_add(1, Ordering::Relaxed);
            write_json(
                &mut writer,
                404,
                &err_json(&format!("no route for {} {}", req.method, req.path)),
            )
        }
    };
    if let Err(e) = result {
        // Usually a client that disconnected mid-response.
        log(&format!("handler io error on {} {}: {}", req.method, req.path, e));
    }
    let _ = writer.shutdown(Shutdown::Both);
}

fn main() {
    let _ = BOOT.set(Instant::now());
    // Panics are contained per-connection by catch_unwind below; the
    // hook just makes them visible in the log with their payload.
    panic::set_hook(Box::new(|info| {
        let loc = info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()))
            .unwrap_or_else(|| "?".to_string());
        let msg = if let Some(s) = info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "unknown".to_string()
        };
        log(&format!("PANIC contained at {}: {}", loc, msg));
    }));

    let state = Arc::new(State::new());
    let listener = match TcpListener::bind(BIND_ADDR) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("bridgekeeper: cannot bind {}: {}", BIND_ADDR, e);
            std::process::exit(1);
        }
    };
    log(&format!(
        "listening on {} (lemond {}, flm {})",
        BIND_ADDR, LEMOND, FLM
    ));

    let poll_state = Arc::clone(&state);
    std::thread::spawn(move || poll_loop(poll_state));

    loop {
        match listener.accept() {
            Ok((stream, _addr)) => {
                let conn_state = Arc::clone(&state);
                // A panicking handler must never kill the listener:
                // the handler runs on its own thread AND inside
                // catch_unwind; on a caught panic we count it, log
                // it, and make a best effort at a 500 on a cloned
                // socket (harmless if a response already started).
                let fallback = stream.try_clone().ok();
                let spawn = std::thread::Builder::new().spawn(move || {
                    let st = Arc::clone(&conn_state);
                    let result = panic::catch_unwind(panic::AssertUnwindSafe(move || {
                        handle_conn(stream, st);
                    }));
                    if result.is_err() {
                        conn_state.panics.fetch_add(1, Ordering::Relaxed);
                        conn_state.errors.fetch_add(1, Ordering::Relaxed);
                        log(&format!(
                            "handler panic contained (total panics: {})",
                            conn_state.panics.load(Ordering::Relaxed)
                        ));
                        if let Some(mut s) = fallback {
                            let _ = write_json(&mut s, 500, &err_json("internal error"));
                            let _ = s.shutdown(Shutdown::Both);
                        }
                    }
                });
                if spawn.is_err() {
                    log("thread spawn failed; connection dropped");
                    state.errors.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(e) => {
                // Accept errors are not fatal: log, brief pause to
                // avoid a hot spin, keep listening.
                log(&format!("accept error: {}", e));
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}
