# Daemon bake-off — one contract, three runtimes

Casey's question: the bridge needs a resident layer in WSL that is
*just there and can't fail*. The study said Rust for daemons, BEAM
only as a control plane, C++ only for an adapter. Those were paper
verdicts. This bake-off replaces them with measurements: the same
daemon, built three ways, raced on the same machine against the
same battery — scored on **robustness first, speed second**.

## The daemon: "bridgekeeper"

A resident process in WSL2 fronting the two host NPU services
(Lemonade at `127.0.0.1:13305` `/api/v1`, FastFlowLM at
`127.0.0.1:53999` `/v1`). Fixed contract for every implementation:

- `GET /health` → `200 {"ok":true, "uptime_s":N, "lemond":"up|down",
  "flm":"up|down", "requests":N, "errors":N}`. Upstream states come
  from polling both services' model-list endpoints every ~2 s.
- `POST /chat {"prompt":"...", "max_tokens":128}` → an SSE stream
  proxying the FLM chat completion (model `llama3.2:1b`): each token
  delta forwarded promptly as `data: {"delta":"..."}`; the stream
  ends with `data: {"done":true}`. **If the upstream dies
  mid-stream, emit `data: {"error":"..."}` and close** — never hang,
  never take the daemon down.
- `POST /embed {"text":"..."}` → `{"dims":768, "embedding":[...]}`
  proxied from Lemonade embeddings (`nomic-embed-text-v1-GGUF`).
  Upstream down → **HTTP 502 JSON error within 2 s**.
- `POST /transcribe` (raw WAV bytes as the body) →
  `{"text":"..."}` via Lemonade Whisper
  (`Whisper-Large-v3-Turbo`, multipart like the Python client).
- Request handlers are isolated units of work: a handler crash must
  never kill the listener. Where the runtime has supervision
  (OTP supervisors, a Rust supervisor task, a C++ watchdog thread),
  use it, and count worker restarts in `/health` if natural.
- No configuration files, no CLI flags: endpoints, model names and
  poll interval are constants. The listen port is the only
  per-implementation difference:
  **Elixir 14111 · Rust 14112 · C++ 14113.**

## Run interface (on the ProArt, WSL)

Everything lives in `~/bakeoff/<lang>/` (`elixir`, `rust`, `cpp`):

- `build.sh` — builds from a clean checkout of that dir, using only
  the toolchain + public registries (crates.io / hex.pm / system
  libs). Must work non-interactively over ssh.
- `run.sh` — starts the daemon in the foreground on its port.

## The battery (identical for all three, plus a no-daemon control)

Run by the orchestrator from `battery.py`; implementations are
black boxes behind their ports.

1. **Correctness** — health shape; streamed chat completes with the
   done marker; embed dims = 768; the bench WAV transcribes to the
   fox text.
2. **Latency** — embed p50/p95 over 50 calls; chat time-to-first-
   token and total, median of 5; compared with direct-to-endpoint
   baselines measured the same run.
3. **Chaos A** — FLM is killed mid-stream (long generation): the
   stream must terminate (error or done) within 10 s, `/health`
   must keep answering, `flm` must read `down`, and chat must work
   again after FLM is restarted; recovery time recorded.
4. **Chaos B** — Lemonade is killed: embed fails fast (502, < 2 s),
   health shows it, FLM chat is unaffected; recovery recorded.
5. **Chaos C** — the daemon itself is `kill -9`'d and restarted via
   `run.sh`: time-to-healthy recorded.
6. **Concurrency** — 8 simultaneous chat streams all complete.
7. **Soak** — 8 minutes of mixed traffic (embed 0.5 Hz, chat
   ~0.05 Hz, health 0.2 Hz): count errors, hangs, and drift in RSS.
8. **Footprint** — idle RSS after warmup, startup time, artifact
   size on disk, rough LOC, dependency count.

## Scoring

Robustness events are pass/fail with times; latency and footprint
are numbers. There is no single weighted score — the report lays
the scorecard out and argues the verdict, including build/operate
effort, because "can't fail" is a property of the whole lifecycle,
not one benchmark.
