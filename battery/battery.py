#!/usr/bin/env python3
"""
battery.py -- the bridgekeeper bake-off battery.

Black-box, identical for every contestant. Runs inside WSL on the
machine the daemons front for. Manages the daemon lifecycle itself
(build.sh -> run.sh -> phases -> kill), and drives the REAL upstream
services through their deaths and rebirths for the chaos phases —
restoring everything it kills, in a finally block, whatever happens.

    python3 battery.py <elixir|rust|cpp> [--soak 480] [--skip-soak]

Writes ~/bakeoff/results/<name>.json and prints a summary. Stdlib
only. Every wait is bounded; a hang is recorded as data, never as a
stuck battery.
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import signal
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

HOME = Path.home()
BAKEOFF = HOME / "bakeoff"
RESULTS = BAKEOFF / "results"
WAV = Path("/mnt/c/Users/casey/AppData/Local/wsl-npu-bridge/bench-sample.wav")
PS = "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe"
FLM_EXE = r"C:\Users\casey\.cache\lemonade\bin\flm\npu\flm.exe"
LEMOND_EXE = (r"C:\Users\casey\AppData\Local\wsl-npu-bridge\lemonade"
              r"\lemonade-embeddable-2026.44.1-windows-x64\lemond.exe")
PORTS = {"elixir": 14111, "rust": 14112, "cpp": 14113}
LEMOND = ("127.0.0.1", 13305)
FLM = ("127.0.0.1", 53999)

CHAT_PROMPT = "In two sentences, explain why a sailboat can sail upwind."
LONG_PROMPT = ("Tell a long, detailed story about a lighthouse keeper "
               "who befriends a whale. Take your time and describe "
               "everything at length.")


# --------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------

def now() -> float:
    return time.perf_counter()


def pct(vals: list[float], q: float) -> float:
    if not vals:
        return float("nan")
    vals = sorted(vals)
    return vals[min(len(vals) - 1, int(q * len(vals)))]


def http(port: int, method: str, path: str, body: bytes | None = None,
         ctype: str = "application/json", timeout: float = 30,
         host: str = "127.0.0.1"):
    """One-shot HTTP; returns (status, body_bytes, elapsed_s)."""
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    start = now()
    try:
        conn.request(method, path, body=body,
                     headers={"Content-Type": ctype} if body else {})
        resp = conn.getresponse()
        data = resp.read()
        return resp.status, data, now() - start
    finally:
        conn.close()


def health(port: int, timeout: float = 5) -> dict | None:
    try:
        status, data, _ = http(port, "GET", "/health", timeout=timeout)
        return json.loads(data) if status == 200 else None
    except Exception:
        return None


def wait_health(port: int, timeout: float = 30) -> float | None:
    """Seconds until /health answers 200, or None on timeout."""
    start = now()
    while now() - start < timeout:
        if health(port, timeout=2):
            return now() - start
        time.sleep(0.25)
    return None


def rss_of_pgid(pgid: int) -> int:
    """Total VmRSS (bytes) of every process in a process group."""
    total = 0
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            if os.getpgid(int(proc.name)) != pgid:
                continue
            for line in (proc / "status").read_text().splitlines():
                if line.startswith("VmRSS:"):
                    total += int(line.split()[1]) * 1024
        except Exception:
            continue
    return total


def ps_cmd(command: str, timeout: float = 60) -> None:
    """Run a PowerShell command on the Windows host (fire-and-verify:
    callers confirm effects by probing endpoints, not by trusting
    this call — WSL interop exit behavior is unreliable)."""
    try:
        subprocess.run([PS, "-NoProfile", "-Command", command],
                       capture_output=True, timeout=timeout)
    except Exception:
        pass


def upstream_alive(which: str) -> bool:
    host, port = LEMOND if which == "lemond" else FLM
    path = "/api/v1/models" if which == "lemond" else "/v1/models"
    try:
        status, _, _ = http(port, "GET", path, timeout=4, host=host)
        return status == 200
    except Exception:
        return False


def wait_upstream(which: str, timeout: float = 60) -> float | None:
    start = now()
    while now() - start < timeout:
        if upstream_alive(which):
            return now() - start
        time.sleep(0.5)
    return None


def kill_upstream(which: str) -> None:
    ps_cmd(f"Stop-Process -Name {'lemond' if which == 'lemond' else 'flm'} "
           f"-Force -ErrorAction SilentlyContinue")


def start_upstream(which: str) -> None:
    if which == "flm":
        ps_cmd(f"Start-Process -FilePath '{FLM_EXE}' -ArgumentList "
               f"'serve llama3.2:1b --host 127.0.0.1 --port 53999' "
               f"-WindowStyle Hidden")
    else:
        ps_cmd(f"Start-Process -FilePath '{LEMOND_EXE}' -ArgumentList "
               f"'--host localhost' -WindowStyle Hidden")


# --------------------------------------------------------------------------
# SSE chat against any endpoint, daemon or direct
# --------------------------------------------------------------------------

def chat_stream(port: int, path: str, payload: dict, timeout: float = 180):
    """Read an SSE chat stream. Returns a dict:
    {ttft_s, total_s, deltas, text, ended: 'done'|'error'|'eof'|'timeout',
     status} — 'timeout' means the stream HUNG (the cardinal sin)."""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    body = json.dumps(payload).encode()
    start = now()
    result = {"ttft_s": None, "total_s": None, "deltas": 0, "text": "",
              "ended": "eof", "status": None}
    try:
        conn.request("POST", path, body=body,
                     headers={"Content-Type": "application/json"})
        resp = conn.getresponse()
        result["status"] = resp.status
        buf = b""
        while True:
            try:
                chunk = resp.read1(4096) if hasattr(resp, "read1") else resp.read(4096)
            except (TimeoutError, OSError):
                result["ended"] = "timeout"
                break
            if not chunk:
                break
            if result["ttft_s"] is None:
                result["ttft_s"] = now() - start
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                line = line.strip()
                if not line.startswith(b"data:"):
                    continue
                data = line[5:].strip()
                if data == b"[DONE]":
                    result["ended"] = "done"
                    continue
                try:
                    obj = json.loads(data)
                except Exception:
                    continue
                # Direct FLM shape: choices[0].delta.content
                delta = None
                if "delta" in obj and isinstance(obj["delta"], str):
                    delta = obj["delta"]  # bridgekeeper shape
                elif obj.get("choices"):
                    delta = obj["choices"][0].get("delta", {}).get("content")
                if obj.get("done"):
                    result["ended"] = "done"
                if obj.get("error"):
                    result["ended"] = "error"
                if delta:
                    result["deltas"] += 1
                    result["text"] += delta
    except (TimeoutError, OSError):
        result["ended"] = "timeout"
    finally:
        result["total_s"] = now() - start
        conn.close()
    return result


def daemon_chat(port: int, prompt: str, max_tokens: int = 128,
                timeout: float = 180) -> dict:
    return chat_stream(port, "/chat",
                       {"prompt": prompt, "max_tokens": max_tokens}, timeout)


def direct_flm_chat(prompt: str, max_tokens: int = 128,
                    timeout: float = 180) -> dict:
    return chat_stream(FLM[1], "/v1/chat/completions",
                       {"model": "llama3.2:1b",
                        "messages": [{"role": "user", "content": prompt}],
                        "max_tokens": max_tokens, "stream": True}, timeout)


def daemon_embed(port: int, timeout: float = 30):
    return http(port, "POST", "/embed",
                body=json.dumps({"text": "a tugboat crossing a cold harbour"}).encode(),
                timeout=timeout)


def direct_embed(timeout: float = 30):
    return http(LEMOND[1], "POST", "/api/v1/embeddings",
                body=json.dumps({"model": "nomic-embed-text-v1-GGUF",
                                 "input": "a tugboat crossing a cold harbour"}).encode(),
                timeout=timeout, host=LEMOND[0])


# --------------------------------------------------------------------------
# the battery
# --------------------------------------------------------------------------

class Battery:
    def __init__(self, lang: str, soak_s: int):
        self.lang = lang
        self.port = PORTS[lang]
        self.soak_s = soak_s
        self.dir = BAKEOFF / lang
        self.proc: subprocess.Popen | None = None
        self.out: dict = {"lang": lang, "port": self.port, "phases": {}}
        self.killed: set[str] = set()

    # -- daemon lifecycle -------------------------------------------------
    def start_daemon(self) -> float | None:
        log = open(RESULTS / f"{self.lang}-daemon.log", "ab")
        self.proc = subprocess.Popen(
            ["bash", "run.sh"], cwd=self.dir, stdout=log, stderr=log,
            start_new_session=True)
        return wait_health(self.port, timeout=60)

    def stop_daemon(self, sig: int = signal.SIGTERM) -> None:
        if self.proc and self.proc.poll() is None:
            try:
                os.killpg(os.getpgid(self.proc.pid), sig)
                self.proc.wait(timeout=10)
            except Exception:
                try:
                    os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
                except Exception:
                    pass
        self.proc = None

    def restore_upstreams(self) -> None:
        for which in self.killed:
            if not upstream_alive(which):
                start_upstream(which)
                wait_upstream(which, timeout=90)
        self.killed.clear()

    # -- phases -------------------------------------------------------------
    def phase_build(self) -> None:
        start = now()
        r = subprocess.run(["bash", "build.sh"], cwd=self.dir,
                           capture_output=True, timeout=900)
        self.out["phases"]["build"] = {
            "seconds": round(now() - start, 2), "ok": r.returncode == 0,
            "tail": r.stdout.decode(errors="replace")[-300:] +
                    r.stderr.decode(errors="replace")[-300:]}
        size = subprocess.run(["du", "-sb", str(self.dir)],
                              capture_output=True, text=True).stdout
        self.out["phases"]["build"]["dir_bytes"] = int(size.split()[0])

    def phase_correctness(self) -> None:
        p: dict = {}
        h = health(self.port)
        p["health_shape"] = bool(h) and all(
            k in h for k in ("ok", "uptime_s", "lemond", "flm",
                             "requests", "errors"))
        status, data, _ = daemon_embed(self.port)
        try:
            p["embed_dims"] = json.loads(data).get("dims")
        except Exception:
            p["embed_dims"] = None
        status, data, _ = http(
            self.port, "POST", "/transcribe", body=WAV.read_bytes(),
            ctype="application/octet-stream", timeout=300)
        try:
            text = json.loads(data).get("text", "")
        except Exception:
            text = ""
        p["transcribe_fox"] = "quick brown fox" in text.lower()
        c = daemon_chat(self.port, CHAT_PROMPT)
        p["chat"] = {"ended": c["ended"], "deltas": c["deltas"],
                     "text_len": len(c["text"])}
        self.out["phases"]["correctness"] = p

    def phase_latency(self) -> None:
        p: dict = {}
        # baselines (direct)
        base_embed = []
        for _ in range(50):
            _, _, el = direct_embed()
            base_embed.append(el)
        base_chat = [direct_flm_chat(CHAT_PROMPT) for _ in range(5)]
        # via daemon
        via_embed = []
        for _ in range(50):
            _, _, el = daemon_embed(self.port)
            via_embed.append(el)
        via_chat = [daemon_chat(self.port, CHAT_PROMPT) for _ in range(5)]
        def chat_stats(rows):
            return {
                "ttft_median_s": round(statistics.median(
                    [r["ttft_s"] for r in rows if r["ttft_s"] is not None]), 3),
                "total_median_s": round(statistics.median(
                    [r["total_s"] for r in rows]), 3),
                "all_done": all(r["ended"] == "done" for r in rows)}
        p["embed_direct"] = {"p50_s": round(pct(base_embed, 0.5), 4),
                             "p95_s": round(pct(base_embed, 0.95), 4)}
        p["embed_daemon"] = {"p50_s": round(pct(via_embed, 0.5), 4),
                             "p95_s": round(pct(via_embed, 0.95), 4)}
        p["embed_added_p50_s"] = round(p["embed_daemon"]["p50_s"] -
                                       p["embed_direct"]["p50_s"], 4)
        p["chat_direct"] = chat_stats(base_chat)
        p["chat_daemon"] = chat_stats(via_chat)
        p["chat_added_ttft_s"] = round(
            p["chat_daemon"]["ttft_median_s"] -
            p["chat_direct"]["ttft_median_s"], 3)
        self.out["phases"]["latency"] = p

    def phase_concurrency(self) -> None:
        results: list[dict] = []
        def worker(i: int) -> None:
            results.append(daemon_chat(
                self.port, f"Count slowly from {i} to {i + 10}.", 64))
        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(8)]
        start = now()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=240)
        self.out["phases"]["concurrency"] = {
            "streams": len(results),
            "all_done": len(results) == 8 and
                        all(r["ended"] == "done" for r in results),
            "wall_s": round(now() - start, 2)}

    def phase_chaos_flm(self) -> None:
        p: dict = {}
        box: dict = {}
        def long_chat() -> None:
            box["r"] = daemon_chat(self.port, LONG_PROMPT, max_tokens=512,
                                   timeout=60)
        t = threading.Thread(target=long_chat)
        t.start()
        time.sleep(2.0)  # let the stream get going
        kill_upstream("flm")
        self.killed.add("flm")
        t.join(timeout=90)
        r = box.get("r", {"ended": "no-result", "total_s": None})
        p["stream_after_kill"] = {"ended": r["ended"],
                                  "total_s": r["total_s"],
                                  "hung": r["ended"] == "timeout"}
        # health must notice, and keep answering
        deadline = now() + 10
        seen_down = None
        while now() < deadline:
            h = health(self.port)
            if h and h.get("flm") == "down":
                seen_down = True
                break
            time.sleep(0.5)
        p["health_noticed_down"] = bool(seen_down)
        # bring FLM back; measure until chat works again through the daemon
        start_upstream("flm")
        wait_upstream("flm", timeout=120)
        t0 = now()
        ok = False
        while now() - t0 < 120:
            c = daemon_chat(self.port, "Say the word ready.", 16, timeout=60)
            if c["ended"] == "done":
                ok = True
                break
            time.sleep(1.0)
        p["recovered"] = ok
        p["recovery_s"] = round(now() - t0, 1) if ok else None
        self.killed.discard("flm")
        self.out["phases"]["chaos_flm"] = p

    def phase_chaos_lemond(self) -> None:
        p: dict = {}
        kill_upstream("lemond")
        self.killed.add("lemond")
        time.sleep(0.5)
        status, _, el = daemon_embed(self.port, timeout=10)
        p["embed_when_down"] = {"status": status, "seconds": round(el, 2),
                                "fast": el < 2.0}
        c = daemon_chat(self.port, "Say the word ready.", 16, timeout=60)
        p["chat_unaffected"] = c["ended"] == "done"
        deadline = now() + 10
        seen_down = None
        while now() < deadline:
            h = health(self.port)
            if h and h.get("lemond") == "down":
                seen_down = True
                break
            time.sleep(0.5)
        p["health_noticed_down"] = bool(seen_down)
        start_upstream("lemond")
        wait_upstream("lemond", timeout=120)
        t0 = now()
        ok = False
        while now() - t0 < 120:
            status, data, _ = daemon_embed(self.port)
            try:
                if status == 200 and json.loads(data).get("dims") == 768:
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(1.0)
        p["recovered"] = ok
        p["recovery_s"] = round(now() - t0, 1) if ok else None
        self.killed.discard("lemond")
        self.out["phases"]["chaos_lemond"] = p

    def phase_chaos_selfkill(self) -> None:
        pgid = os.getpgid(self.proc.pid)
        os.killpg(pgid, signal.SIGKILL)
        time.sleep(0.5)
        t = self.start_daemon()
        self.out["phases"]["chaos_selfkill"] = {
            "time_to_healthy_s": round(t, 2) if t is not None else None}

    def phase_soak(self) -> None:
        if self.soak_s <= 0:
            self.out["phases"]["soak"] = {"skipped": True}
            return
        pgid = os.getpgid(self.proc.pid)
        rss_start = rss_of_pgid(pgid)
        errors = hangs = embeds = chats = healths = 0
        start = now()
        next_embed = next_chat = next_health = start
        while now() - start < self.soak_s:
            t = now()
            if t >= next_health:
                healths += 1
                if not health(self.port):
                    errors += 1
                next_health = t + 5
            if t >= next_embed:
                embeds += 1
                try:
                    status, _, el = daemon_embed(self.port, timeout=15)
                    if status != 200:
                        errors += 1
                    if el > 10:
                        hangs += 1
                except Exception:
                    errors += 1
                next_embed = t + 2
            if t >= next_chat:
                chats += 1
                c = daemon_chat(self.port, CHAT_PROMPT, 96, timeout=120)
                if c["ended"] != "done":
                    errors += 1
                next_chat = t + 30
            time.sleep(0.2)
        rss_end = rss_of_pgid(pgid)
        self.out["phases"]["soak"] = {
            "seconds": self.soak_s, "embeds": embeds, "chats": chats,
            "healths": healths, "errors": errors, "hangs": hangs,
            "rss_start_mb": round(rss_start / 1e6, 1),
            "rss_end_mb": round(rss_end / 1e6, 1)}

    # -- driver -------------------------------------------------------------
    def run(self) -> dict:
        RESULTS.mkdir(parents=True, exist_ok=True)
        try:
            self.phase_build()
            t = self.start_daemon()
            self.out["startup_s"] = round(t, 2) if t is not None else None
            if t is None:
                self.out["fatal"] = "daemon never became healthy"
                return self.out
            self.out["idle_rss_mb"] = round(
                rss_of_pgid(os.getpgid(self.proc.pid)) / 1e6, 1)
            self.phase_correctness()
            self.phase_latency()
            self.phase_concurrency()
            self.phase_chaos_flm()
            self.phase_chaos_lemond()
            self.phase_chaos_selfkill()
            self.phase_soak()
        finally:
            self.stop_daemon()
            self.restore_upstreams()
        return self.out


def main() -> int:
    parser = argparse.ArgumentParser(description="bridgekeeper bake-off battery")
    parser.add_argument("lang", choices=sorted(PORTS))
    parser.add_argument("--soak", type=int, default=480,
                        help="soak seconds (0 = skip)")
    args = parser.parse_args()
    battery = Battery(args.lang, args.soak)
    out = battery.run()
    RESULTS.mkdir(parents=True, exist_ok=True)
    dest = RESULTS / f"{args.lang}.json"
    dest.write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))
    print(f"\nwrote {dest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
