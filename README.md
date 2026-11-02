# bridgekeeper-bakeoff

**One daemon contract. Three runtimes. Real hardware. Robustness
first, speed second.**

The [wsl-npu-bridge](https://github.com/si-ready/wsl-npu-bridge)
needs a resident layer in WSL2 that fronts the host NPU services
and is *just there and can't fail*. A prior study gave paper
verdicts — Rust for daemons, Elixir/BEAM only as a control plane,
C++ only for an adapter. This repo exists to replace paper with
measurements: the same daemon, built three ways, raced on the same
machine against the same black-box battery.

- **The contract:** [`SPEC.md`](SPEC.md) — health with polled
  upstream state, SSE streaming chat proxy (upstream death must
  become an error event, never a hang), embeddings, transcription,
  handler crashes must never kill the listener.
- **The contestants:** [`elixir/`](elixir/) (OTP supervision,
  zero deps) · [`rust/`](rust/) (std-only, catch_unwind per
  connection thread) · [`cpp/`](cpp/) (process-per-request fork
  isolation, mmap'd atomic state). Each has a `NOTES.md` arguing
  its own design and naming its gaps.
- **The battery:** [`battery/battery.py`](battery/battery.py) —
  correctness, latency vs a no-daemon direct control, 8-way
  concurrent streams, and chaos: FLM killed mid-stream, Lemonade
  killed, the daemon itself `kill -9`'d, then an 8-minute soak.
  It restores everything it kills.
- **The results:** [`results/`](results/) — one JSON scorecard
  per contestant plus the written verdict.

Run on an ASUS ProArt PX13 (Ryzen AI 9 HX 370) in WSL2, against
the live bridge services (Lemonade + FastFlowLM on the NPU).

## Why here and not in si-ready

si-ready is the curated shelf: what an average person can grab and
go. The graduated winner of this bake-off belongs there (as part
of wsl-npu-bridge). The race itself — three implementations, a
chaos battery, the dead ends — is SuperInstance material: the
sketchbook where the process is the point.
