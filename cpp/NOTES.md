# bridgekeeper — C++ contestant: design notes

One C++17 file (`main.cpp`, 672 lines), raw POSIX sockets for the
server side, libcurl for the upstream side, nlohmann/json for JSON.
Listens on **14113** and implements the SPEC contract exactly:
`/health` (2 s upstream polling), SSE `/chat` proxying FLM,
`/embed` proxying Lemonade embeddings, `/transcribe` proxying
Lemonade Whisper.

## Failure containment: process-per-request (the honest C++ answer)

Threads share an address space, so a thread-per-request C++ daemon
cannot honestly claim "a handler crash never kills the listener" —
one segfault takes every thread with it. This implementation
therefore uses the containment unit the OS actually enforces:

* **The listener process only `accept()`s and `fork()`s.** It never
  parses a request, never touches JSON, never calls libcurl. There
  is no code path by which request content reaches it.
* **Every request is handled by a short-lived child process.** A
  handler that segfaults, aborts, throws, or hangs dies alone; the
  client sees a dropped connection, the listener keeps serving.
  Children get a 900 s `alarm()` backstop so a wedged handler
  cannot linger forever.
* **The parent reaps children via a SIGCHLD handler** and counts
  signal-killed workers in `/health` as `worker_crashes` (also
  counted in `errors`). This is the C++ analogue of the SPEC's
  supervisor-restart counter: workers aren't restarted, they're
  respawned per request, and the crash count is the visible metric.
* **A separate poller child** probes both upstreams' model-list
  endpoints on a ~2 s period and publishes `lemond`/`flm` state.
* **All shared state** (upstream flags, `requests`, `errors`,
  `worker_crashes`, start time) lives in one `mmap(MAP_SHARED)`
  page, touched only through lock-free `__atomic` builtins — a
  crashing child cannot corrupt or lose the bookkeeping.
* **No threads anywhere.** Forking from a single-threaded parent
  sidesteps the fork-in-a-threaded-process hazards entirely, and
  makes the 8-simultaneous-streams case trivially parallel:
  8 children, 8 processes, no shared mutable state to race on.
* Belt-and-braces inside the child: exceptions are caught at the
  handler boundary, upstream JSON is parsed defensively (a malformed
  SSE chunk from FLM is skipped, not fatal), `SIGPIPE` is ignored
  and sends use `MSG_NOSIGNAL` (a client vanishing mid-stream just
  ends that child).

**The honest limit:** containment is per-request, not per-daemon.
If the *listener* itself is killed (Chaos C), only an external
restart (`run.sh` again, a supervisor, systemd) brings it back —
C++ gives you no in-process supervision story for the supervisor
itself. Fork-per-request also costs more per connection than a
thread would (a few ms); at this workload (inference dominates by
orders of magnitude) that's noise, but it is a real trade, recorded
here rather than hidden.

## Failure semantics, per endpoint

* `/chat`: SSE headers are sent *before* the upstream call, so every
  upstream failure is reported in-band: connect failure, HTTP error,
  a dropped connection mid-stream (curl error), or a stream that
  ends without FLM's `[DONE]` all produce
  `data: {"error":"..."}` and a clean close. Never a hang: curl's
  low-speed limit aborts a fully silent upstream after 120 s, the
  child alarm after 900 s.
* `/embed`: 1 s connect timeout — a dead Lemonade yields
  **HTTP 502 JSON in well under the 2 s budget** (refused local
  connects return in ~1 ms). Live-but-broken upstream responses
  (non-200, unparseable) also map to 502 with the reason.
* `/transcribe`: same 502 mapping; generous 600 s total timeout for
  cold Whisper loads.
* Bad JSON / missing fields → 400 JSON; unknown routes → 404 JSON.

## Dependencies (2, chosen consciously)

| Dep | Why |
|---|---|
| **libcurl** (system, OpenSSL flavour) | The upstream client needs streaming writes, multipart MIME (Whisper), connect/total timeouts and careful error taxonomy. Hand-rolling that correctly is where robustness goes to die; libcurl is the most battle-tested HTTP client in existence. |
| **nlohmann/json** (header-only) | JSON string escaping is exactly the code you don't want hand-rolled in a crash-containment story. Header-only: zero runtime deps beyond libcurl + libc/libstdc++. |

Server side is hand-rolled POSIX sockets on purpose: the HTTP/1.1
surface needed here is tiny (Content-Length requests, close-after-
response, one SSE stream), and keeping it in-tree means the
listener path has no library between it and `accept()`.

Build-time deps (dev packages only): `libcurl4-openssl-dev`,
`nlohmann-json3-dev`. Runtime deps: `libcurl4t64`, libc, libstdc++.

## Measured on the ProArt (2026-11-01, this build)

* **Build time:** 3.6 s (`g++ -O2 -std=c++17`), zero warnings with
  `-Wall -Wextra`.
* **Binary size:** 220,096 bytes (~216 KiB), dynamically linked.
* **Startup:** 124 ms from launch to the first `/health` answering
  with both upstreams already reported `up`.
* **Idle RSS:** listener 13.3 MB + poller 4.7 MB ≈ **18 MB** total
  (includes mapped libcurl/OpenSSL pages).
* **Smoke (all against the live host services):**
  * `/chat` streamed real FLM deltas token-by-token, terminated
    with `data: {"done":true}`.
  * `/embed` returned `dims: 768` and a real vector.
  * `/transcribe` of the bench WAV returned the full "quick brown
    fox" text in 3.4 s end-to-end.
  * Error paths exercised: bad JSON → 400, missing field → 400,
    unknown route → 404, garbage request line → 404, client
    aborting a long stream at 1 s → stream died with the client,
    daemon unaffected, `worker_crashes` stayed 0.

## Known gaps

* **HTTP/1.1 subset:** request bodies must arrive with
  `Content-Length` (no chunked request decoding); responses are
  `Connection: close` (no keep-alive). The SPEC battery and curl
  both speak this subset; a general-purpose client that insists on
  chunked uploads would not.
* **No TLS anywhere** (loopback-only bridge, per SPEC).
* **`/health` adds one extra field**, `worker_crashes`, beyond the
  SPEC shape — the SPEC explicitly invites a restart counter
  "if natural"; all six specified fields are present with the
  specified names and types.
* **Fork cost under extreme connection rates** is unmeasured; the
  soak test will show whether it matters at battery scale
  (~0.75 requests/s mixed — it should not).
* The daemon logs only its startup line; handler children are
  silent by design (their outcomes are in the counters). A
  production version would want the poller's state transitions
  logged with timestamps.
