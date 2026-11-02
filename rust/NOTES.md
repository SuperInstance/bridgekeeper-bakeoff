# bridgekeeper — Rust contestant: notes

## Design

One file, `src/main.rs` (~940 lines), **zero dependencies**. A
`TcpListener` on the main thread accepts connections; each
connection gets its own OS thread; a third thread polls both
upstreams' model-list endpoints every 2 s into two atomics that
`/health` reads. All upstream calls are blocking `TcpStream`s with
explicit connect/read timeouts. Responses to clients are
`Connection: close`, one request per connection — deliberately the
simplest HTTP that satisfies the contract.

## Why std-only instead of axum + tokio + reqwest

The scorecard counts dependencies, build time, binary size, and
startup time, and the contract's failure rules are all socket-level
behaviors (fail fast, error event instead of hang). An async stack
would have scored worse on every footprint metric (~200 transitive
crates, minutes of build, >10 MB binary) while buying nothing on
the robustness axis: tokio timeouts and std socket timeouts enforce
the same deadlines, and a wedged async executor is a failure mode
the threaded design simply doesn't have. The price is hand-rolled
HTTP framing and field-extraction JSON (~350 of the 940 lines) —
paid once, in the open, with no supply chain. If this daemon ever
needs TLS, HTTP/2, or thousands of concurrent streams, that
trade-off flips and axum becomes the right call; at 8 concurrent
streams on loopback it does not.

Measured on the ProArt (WSL2, rustc 1.93.1):

| Metric | Value |
|---|---|
| Clean build time (`rm -rf target && ./build.sh`) | **3.0 s** |
| Binary size (stripped, LTO) | **441,816 B (432 KB)** |
| Idle RSS after warmup | **2,684 KB** |
| Startup to listening | < 0.5 s (fork+exec dominated; bind is immediate) |
| Dependencies | **0** |
| Source size | 941 lines incl. comments (one file) |

## Failure isolation (the deliberate part)

- Every connection handler runs inside `panic::catch_unwind` on its
  own thread. A panic kills at most that connection: the catch site
  increments `panics`/`errors`, logs via a custom panic hook, and
  makes a best-effort attempt at a 500 on a cloned socket (harmless
  if a response already started). `panic = "unwind"` is pinned in
  Cargo.toml — an `abort` profile would silently defeat this.
- Accept-loop errors are logged and retried after 50 ms; a failed
  thread spawn drops one connection, never the listener.
- `/chat` commits to SSE headers first, then *every* failure —
  upstream unreachable, non-200, mid-stream RST, idle timeout
  (120 s per read) — is delivered as `data: {"error":"..."}` plus
  close. There is no code path that can hang a stream: each read is
  bounded by a socket timeout, and a clean EOF before `[DONE]`
  becomes either `done` (if a `finish_reason` was seen) or an error
  event.
- `/embed` with lemond down: loopback connection-refused returns in
  milliseconds, far inside the 2 s bound; a wedged-but-listening
  lemond is bounded by a 1 s connect / 30 s read timeout → 502.
- If the client disconnects mid-stream, the write fails, the
  handler returns, and dropping the upstream socket tells FLM to
  stop generating for a dead client.
- Counters: `requests` counts every routed request; `errors`
  counts contract failures (400/404/500/502, SSE error events,
  panics). Client-initiated disconnects are not errors.

## JSON without serde

The contract needs five fields out of three upstream payloads.
`extract_string`/`extract_int` do key search + a lenient string
parser (full escape handling incl. surrogate pairs); the
768-float embedding array is copied through as raw text with its
elements counted and its character set validated — zero float
parsing, one substring copy of a 16 KB body.

## Known gaps / deliberate simplifications

- **No HTTP keep-alive** (one request per connection). Loopback
  reconnect cost is sub-millisecond; keep-alive was complexity the
  contract doesn't reward.
- **Key search is textual**: `find_key_value_start` can in theory
  be fooled by a prompt containing a literal `"max_tokens":` —
  battery-shaped inputs are natural language; noted, accepted.
- **Request bodies**: Content-Length and chunked are both decoded
  (the same `ChunkedReader` serves both directions); bodies are
  capped at 256 MB.
- **`/health` reports exactly the SPEC keys.** Panic totals are in
  the stderr log, not `/health`, to keep the health shape exact.
- Startup state is `down` until the first poll lands (~instantly);
  a `/health` in the first milliseconds can briefly read `down`
  for a live upstream.
- SSE parsing of FLM output assumes one JSON object per `data:`
  line, which is what FLM emits (verified against the live
  service). Multi-line data fields would be forwarded unparsed —
  FLM doesn't produce them.

## Smoke evidence (2026-11-01, live upstreams)

- `/health` → `{"ok":true,"uptime_s":67,"lemond":"up","flm":"up","requests":1,"errors":0}`
- `/chat` → `data: {"delta":"hello"}` … `data: {"done":true}`; a
  counting prompt produced 9 per-token deltas, promptly flushed.
- `/embed` → `{"dims":768,"embedding":[0.00666341…` — values
  bit-identical to a direct call against lemond.
- `/transcribe` (bench fixture WAV) → the full "quick brown fox …
  Windows subsystem for Linux" text.
- 4 concurrent chat streams all completed with `done`.
- 404/400 shapes verified; after the smoke set, `/health` showed
  `requests:12, errors:2` (the deliberate 404 + 400).
