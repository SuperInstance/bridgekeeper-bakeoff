# bridgekeeper — Elixir/BEAM entry

Port **14111**. Zero hex dependencies: the whole daemon is OTP
stdlib (`:gen_tcp`, `:counters`, ETS) plus hand-rolled HTTP, SSE
and JSON written for this contract. If the BEAM route wins or
loses this bake-off, it will not be because of a library.

## Design

```
Bridgekeeper.Supervisor (one_for_one)
├── Bridgekeeper.State      counters + upstream poller
├── Task.Supervisor        one task per client connection
└── Bridgekeeper.Listener  owns the listen socket + acceptor loop
```

- **Listener/acceptors.** The Listener GenServer owns the listen
  socket; a linked acceptor loop accepts and hands each socket to
  a fresh task under the Task.Supervisor (`:gen_tcp.controlling_process`
  transfers ownership). If the acceptor dies, the listener dies
  with it and the supervisor rebuilds both with a fresh socket.
- **Handlers are disposable.** Each connection runs in its own
  supervised task with a catch-all boundary: any exception is
  counted in `errors`, turned into a best-effort 500, and the
  socket closes. Verified live: raw garbage bytes and malformed
  JSON were thrown at the port mid-traffic; `/health` never
  blinked and concurrent streams completed.
- **State without contention.** `requests`/`errors` are atomic
  `:counters`; upstream `up|down` lives in a public ETS table.
  `/health` reads both directly — it never calls into the poller,
  so a hung upstream cannot stall a health read. Polls (GET
  /models on both services, every 2 s) each run in a spawned
  throwaway process with a 1.5 s budget.
- **Upstream client.** One socket per call, `Connection: close`,
  no pool: pooled connections are exactly the state that goes
  stale when an upstream is killed and restarted under you. The
  client speaks all three HTTP body framings (content-length,
  chunked, close-delimited) through a byte pump with two strictly
  separate buffers — `raw` for de-chunking, `decoded` for line
  splitting. SSE lines are assembled from decoded bytes, so chunk
  boundaries and TCP segmentation are invisible to the parser.
- **Chat streaming.** SSE headers go out immediately; from then on
  every failure — connect refused, bad status, socket death
  mid-generation, 120 s of silence — is reported *in-stream* as
  `data: {"error":"..."}` followed by close. There is no code path
  that waits forever: every socket read in the daemon carries a
  timeout. Clean completion forwards each token delta as it
  arrives and ends with `data: {"done":true}`.
- **Embed.** The 768-float array is spliced *verbatim* out of the
  upstream response body (balanced-bracket scan; elements are
  scalars, so depth counting is exact) — no float round-trip, no
  precision loss, O(n) with no allocation storm. A full JSON
  decode is the fallback. Per-read budget 1.5 s: a dead lemond is
  a 502 in milliseconds (connection refused), per the contract.
- **Transcribe.** The raw WAV body is re-wrapped in a hand-built
  multipart body identical in shape to the Python client's, with a
  180 s read budget (Whisper cold loads measured ~32 s on this
  machine; warm transcription of the 13.2 s fixture took ~3 s
  end-to-end through the daemon).

## Files (~1,160 LOC incl. docs)

| File | What |
|---|---|
| `lib/bridgekeeper/application.ex` | supervision tree |
| `lib/bridgekeeper/state.ex` | counters, ETS, poller |
| `lib/bridgekeeper/server.ex` | listener, acceptor, handlers, routes |
| `lib/bridgekeeper/upstream.ex` | gen_tcp HTTP client + SSE line reader |
| `lib/bridgekeeper/json.ex` | full JSON codec (escapes, surrogate pairs) |
| `mix.exs` | project; `deps: []` |
| `build.sh` | `MIX_ENV=prod mix release` (~1.1 s from clean, measured) |
| `run.sh` | release `start` in the foreground (see surprises) |

## Measured on the ProArt (this build, smoke-level)

- Clean build (`rm -rf _build`): **1.06 s**. Startup to first
  healthy `/health`: **0.53 s**.
- Idle RSS: **~150 MB** — see surprises; it did not grow under
  smoke traffic (144 MB after the full smoke sequence).
- Release size on disk: **18 MB**. Dependency count: **0**.
- Smoke: `/health` shape exact; chat streamed real deltas ending
  in `{"done":true}`; embed returned dims 768; the bench WAV
  transcribed word-perfect; 4 concurrent chat streams completed.

## Surprises (the honest section)

1. **Idle RSS is ~150 MB on this 24-core box.** BEAM preallocates
   per-scheduler allocator arenas; 24 schedulers in WSL make the
   resident set look fat next to a C++ daemon before any work
   happens. It is flat, not leaky — but the bake-off should record
   it as the BEAM's entry fee on many-core machines. The levers
   exist (release `vm.args`, scheduler counts) but were left
   untouched: the contract says no configuration, and a tuned
   number would measure my tuning, not the runtime's default
   character.
2. **Mix release scripts have no `foreground` command** (that's
   other tooling); `start` *is* the foreground mode and `daemon`
   is the detached one. First launch failed on exactly this.
3. **`packet: :http` header values arrive in Erlang's choice of
   shape** — the request path came back as a charlist where a
   binary was expected (404'd every route until normalized), and
   `Content-Length` arrived stringified, which silently routed
   every POST into the body-too-large clause: empty replies, zero
   log output. Both are one-line normalizations now, documented
   in the code, but they cost real debugging time and neither is
   discoverable without raw-socket bisection.
4. **A non-streaming response's head only arrives after the
   upstream finishes the work.** My first upstream client had a
   fixed 2 s head timeout — fine for models/embeddings, fatal for
   Whisper (2.9 s warm). Head waits now inherit the caller's
   budget; streaming heads get 30 s for model loads.
5. Nothing about OTP supervision surprised me, which is itself
   the finding: the crash-isolation story worked as advertised on
   the first try, including the parts (acceptor death → listener
   restart, handler garbage) I could test without touching the
   upstreams.

## Known gaps

- **Request bodies must arrive with `Content-Length`.** Chunked
  *request* bodies are not decoded (responses are). The battery's
  clients set content-length; a general-purpose server would need
  this fixed.
- **Embed's 1.5 s budget** can false-fail a *cold* lemond (first
  embeddings call after a lemond restart loads the model, which
  takes seconds). Warm calls are ~11 ms. Fail-fast when down was
  the contract's explicit demand; the trade is recorded here.
- **One request per connection** (`Connection: close` everywhere).
  Correct and simple; a keep-alive battery would measure extra
  TCP setups. Localhost setup cost is small and equal-ish across
  implementations that make the same choice.
- Handler crash *restarts* are not counted in `/health` (the
  contract fixes the health shape); handler failures are counted
  in `errors`. Worker processes are `:temporary` by design —
  a dead handler's client is gone, so restarting it is meaningless.
- The two-buffer pump is the subtlest code here and has no unit
  tests in this entry; it was verified against FLM's live chunked
  SSE and Lemonade's content-length responses only.
