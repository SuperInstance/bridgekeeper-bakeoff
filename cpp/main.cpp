// bridgekeeper -- C++ contestant in the daemon bake-off (SPEC.md).
//
// A resident WSL2 daemon fronting the two host NPU services:
//   Lemonade  http://127.0.0.1:13305/api/v1   (embeddings, whisper)
//   FastFlowLM http://127.0.0.1:53999/v1      (streaming chat)
//
// Containment design (see NOTES.md for the full reasoning):
//   * The listener process does nothing but accept() and fork().
//   * Every request is handled by a short-lived child process.
//     A handler that segfaults, aborts, or hangs (backstop alarm)
//     dies alone; the listener never shares its fate because it
//     never runs handler code. The parent counts signal-killed
//     workers in /health as "worker_crashes".
//   * A separate poller child probes both upstreams every ~2 s and
//     publishes their state into a shared mmap page that /health
//     reads. Counters (requests/errors) live in the same page and
//     are updated with lock-free __atomic builtins, so a crashed
//     child cannot lose or corrupt the parent's bookkeeping.
//   * No threads anywhere: fork() from a single-threaded parent is
//     the whole concurrency story, which also makes the 8-way
//     concurrent-stream case trivially parallel (8 children).
//
// Dependencies: libcurl (upstream HTTP client) + nlohmann/json.
// Everything else is POSIX/libc. Rationale in NOTES.md.

#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string>

#include <curl/curl.h>
#include <nlohmann/json.hpp>

using nlohmann::json;

// ------------------------------------------------------------------
// Constants (SPEC: no config files, no CLI flags)
// ------------------------------------------------------------------
static const int    kPort        = 14113;
static const char*  kLemondBase  = "http://127.0.0.1:13305";
static const char*  kFlmBase     = "http://127.0.0.1:53999";
static const char*  kChatModel   = "llama3.2:1b";
static const char*  kEmbedModel  = "nomic-embed-text-v1-GGUF";
static const char*  kSttModel    = "Whisper-Large-v3-Turbo";
static const size_t kMaxBody     = 64u << 20;   // 64 MiB request cap
static const int    kChildAlarmS = 900;         // hung-handler backstop

// ------------------------------------------------------------------
// Shared state: one mmap page shared by listener, poller, handlers.
// All fields are accessed only through __atomic builtins.
// ------------------------------------------------------------------
struct Shared {
    long start_epoch;
    long requests;
    long errors;
    long worker_crashes;
    int  lemond_up;
    int  flm_up;
};
static Shared* g_sh = nullptr;

static long sh_load_long(long* p) { return __atomic_load_n(p, __ATOMIC_SEQ_CST); }
static void sh_add_long(long* p, long v) { __atomic_add_fetch(p, v, __ATOMIC_SEQ_CST); }
static int  sh_load_int(int* p) { return __atomic_load_n(p, __ATOMIC_SEQ_CST); }
static void sh_set_int(int* p, int v) { __atomic_store_n(p, v, __ATOMIC_SEQ_CST); }

// ------------------------------------------------------------------
// Small socket helpers
// ------------------------------------------------------------------
static bool send_all(int fd, const char* data, size_t len) {
    while (len > 0) {
        ssize_t n = send(fd, data, len, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        data += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}
static bool send_all(int fd, const std::string& s) {
    return send_all(fd, s.data(), s.size());
}

static std::string http_status_line(int status, const char* reason) {
    char buf[64];
    std::snprintf(buf, sizeof buf, "HTTP/1.1 %d %s\r\n", status, reason);
    return buf;
}

static void send_json(int fd, int status, const char* reason,
                      const std::string& body) {
    std::string head = http_status_line(status, reason);
    head += "Content-Type: application/json\r\n";
    head += "Content-Length: " + std::to_string(body.size()) + "\r\n";
    head += "Connection: close\r\n\r\n";
    send_all(fd, head);
    send_all(fd, body);
}

static void send_error(int fd, int status, const char* reason,
                       const std::string& message) {
    sh_add_long(&g_sh->errors, 1);
    send_json(fd, status, reason,
              json{{"error", message}}.dump());
}

// Read one HTTP request: returns false if the connection is unusable.
// Only Content-Length bodies are supported (the battery and curl both
// send one); chunked request bodies get a clean 400 from the caller
// when content_length stays -1 on a POST.
struct Request {
    std::string method;
    std::string path;
    long content_length = -1;
    std::string body;
};

static bool read_request(int fd, Request& req) {
    std::string buf;
    char tmp[8192];
    // Read until end of headers.
    size_t hdr_end = std::string::npos;
    while (buf.size() <= (64u << 10)) {
        hdr_end = buf.find("\r\n\r\n");
        if (hdr_end != std::string::npos) break;
        ssize_t n = recv(fd, tmp, sizeof tmp, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) return false;
        buf.append(tmp, static_cast<size_t>(n));
    }
    if (hdr_end == std::string::npos) return false;

    std::string head = buf.substr(0, hdr_end);
    std::string rest = buf.substr(hdr_end + 4);

    // Request line.
    size_t sp1 = head.find(' ');
    size_t sp2 = (sp1 == std::string::npos) ? sp1 : head.find(' ', sp1 + 1);
    if (sp1 == std::string::npos || sp2 == std::string::npos) return false;
    req.method = head.substr(0, sp1);
    req.path = head.substr(sp1 + 1, sp2 - sp1 - 1);
    // Strip any query string; the contract has none.
    size_t q = req.path.find('?');
    if (q != std::string::npos) req.path = req.path.substr(0, q);

    // Headers (case-insensitive Content-Length scan).
    size_t pos = head.find("\r\n");
    while (pos != std::string::npos) {
        size_t next = head.find("\r\n", pos + 2);
        std::string line = head.substr(
            pos + 2, (next == std::string::npos ? head.size() : next) - pos - 2);
        size_t colon = line.find(':');
        if (colon != std::string::npos) {
            std::string name = line.substr(0, colon);
            for (auto& c : name) c = static_cast<char>(std::tolower(c));
            if (name == "content-length") {
                req.content_length = std::strtol(line.c_str() + colon + 1,
                                                 nullptr, 10);
            }
        }
        if (next == std::string::npos) break;
        pos = next;
    }

    // Body.
    req.body = rest;
    if (req.content_length > 0) {
        if (req.content_length > static_cast<long>(kMaxBody)) return false;
        while (req.body.size() < static_cast<size_t>(req.content_length)) {
            ssize_t n = recv(fd, tmp, sizeof tmp, 0);
            if (n < 0) {
                if (errno == EINTR) continue;
                return false;
            }
            if (n == 0) return false;
            req.body.append(tmp, static_cast<size_t>(n));
        }
        req.body.resize(static_cast<size_t>(req.content_length));
    }
    return true;
}

// ------------------------------------------------------------------
// Upstream helpers (libcurl)
// ------------------------------------------------------------------
static size_t collect_cb(char* ptr, size_t size, size_t nmemb, void* ud) {
    auto* out = static_cast<std::string*>(ud);
    out->append(ptr, size * nmemb);
    return size * nmemb;
}

// GET probe used by the poller: true iff HTTP 200.
static bool probe_up(const char* url) {
    CURL* c = curl_easy_init();
    if (!c) return false;
    std::string sink;
    curl_easy_setopt(c, CURLOPT_URL, url);
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, &collect_cb);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, &sink);
    curl_easy_setopt(c, CURLOPT_CONNECTTIMEOUT_MS, 700L);
    curl_easy_setopt(c, CURLOPT_TIMEOUT_MS, 1500L);
    curl_easy_setopt(c, CURLOPT_NOSIGNAL, 1L);
    CURLcode rc = curl_easy_perform(c);
    long code = 0;
    if (rc == CURLE_OK) curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &code);
    curl_easy_cleanup(c);
    return rc == CURLE_OK && code == 200;
}

// ------------------------------------------------------------------
// Handlers (each runs in its own forked child process)
// ------------------------------------------------------------------
static void handle_health(int fd) {
    long up = static_cast<long>(std::time(nullptr)) - sh_load_long(&g_sh->start_epoch);
    json j;
    j["ok"] = true;
    j["uptime_s"] = up;
    j["lemond"] = sh_load_int(&g_sh->lemond_up) ? "up" : "down";
    j["flm"] = sh_load_int(&g_sh->flm_up) ? "up" : "down";
    j["requests"] = sh_load_long(&g_sh->requests);
    j["errors"] = sh_load_long(&g_sh->errors);
    j["worker_crashes"] = sh_load_long(&g_sh->worker_crashes);
    send_json(fd, 200, "OK", j.dump());
}

// ---- /chat: SSE proxy to FLM --------------------------------------
struct ChatCtx {
    int fd;
    std::string buf;          // partial SSE line accumulator
    bool client_gone = false;
    bool got_done = false;    // saw upstream [DONE]
};

static bool chat_emit(ChatCtx& ctx, const std::string& payload_json) {
    std::string frame = "data: " + payload_json + "\n\n";
    if (!send_all(ctx.fd, frame)) {
        ctx.client_gone = true;
        return false;
    }
    return true;
}

static void chat_process_line(ChatCtx& ctx, const std::string& line) {
    // FLM speaks OpenAI SSE: lines of "data: {...}" / "data: [DONE]".
    if (line.rfind("data:", 0) != 0) return;
    std::string payload = line.substr(5);
    size_t b = payload.find_first_not_of(" \t\r");
    if (b == std::string::npos) return;
    payload = payload.substr(b);
    if (payload == "[DONE]") {
        ctx.got_done = true;
        return;
    }
    try {
        json j = json::parse(payload);
        auto choices = j.value("choices", json::array());
        if (choices.empty()) return;
        auto delta = choices[0].value("delta", json::object());
        if (delta.contains("content") && delta["content"].is_string()) {
            std::string content = delta["content"].get<std::string>();
            if (!content.empty()) {
                chat_emit(ctx, json{{"delta", content}}.dump());
            }
        }
    } catch (...) {
        // A malformed upstream chunk is skipped, never fatal: the
        // stream contract is about liveness, not per-chunk purity.
    }
}

static size_t chat_write_cb(char* ptr, size_t size, size_t nmemb, void* ud) {
    auto* ctx = static_cast<ChatCtx*>(ud);
    size_t total = size * nmemb;
    if (ctx->client_gone) return 0;  // aborts the curl transfer
    ctx->buf.append(ptr, total);
    size_t nl;
    while ((nl = ctx->buf.find('\n')) != std::string::npos) {
        std::string line = ctx->buf.substr(0, nl);
        ctx->buf.erase(0, nl + 1);
        chat_process_line(*ctx, line);
        if (ctx->client_gone) return 0;
    }
    return total;
}

static void handle_chat(int fd, const Request& req) {
    std::string prompt;
    long max_tokens = 256;
    try {
        json j = json::parse(req.body);
        if (!j.contains("prompt") || !j["prompt"].is_string()) {
            send_error(fd, 400, "Bad Request", "missing string field: prompt");
            return;
        }
        prompt = j["prompt"].get<std::string>();
        if (j.contains("max_tokens") && j["max_tokens"].is_number_integer()) {
            max_tokens = j["max_tokens"].get<long>();
            if (max_tokens < 1) max_tokens = 1;
            if (max_tokens > 8192) max_tokens = 8192;
        }
    } catch (...) {
        send_error(fd, 400, "Bad Request", "request body is not valid JSON");
        return;
    }

    // SSE headers go out before the upstream call so a dead upstream
    // is reported in-band (error event), never as a hung request.
    std::string head = http_status_line(200, "OK");
    head += "Content-Type: text/event-stream\r\n";
    head += "Cache-Control: no-cache\r\n";
    head += "Connection: close\r\n\r\n";
    if (!send_all(fd, head)) return;

    json up;
    up["model"] = kChatModel;
    up["messages"] = json::array({{{"role", "user"}, {"content", prompt}}});
    up["max_tokens"] = max_tokens;
    up["stream"] = true;
    std::string up_body = up.dump();

    ChatCtx ctx;
    ctx.fd = fd;
    CURL* c = curl_easy_init();
    if (!c) {
        sh_add_long(&g_sh->errors, 1);
        chat_emit(ctx, json{{"error", "failed to initialise HTTP client"}}.dump());
        return;
    }
    struct curl_slist* hdrs = nullptr;
    hdrs = curl_slist_append(hdrs, "Content-Type: application/json");
    std::string url = std::string(kFlmBase) + "/v1/chat/completions";
    curl_easy_setopt(c, CURLOPT_URL, url.c_str());
    curl_easy_setopt(c, CURLOPT_HTTPHEADER, hdrs);
    curl_easy_setopt(c, CURLOPT_POSTFIELDS, up_body.c_str());
    curl_easy_setopt(c, CURLOPT_POSTFIELDSIZE, static_cast<long>(up_body.size()));
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, &chat_write_cb);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, &ctx);
    curl_easy_setopt(c, CURLOPT_CONNECTTIMEOUT_MS, 2000L);
    // No total timeout for a token stream; abort only if the upstream
    // goes completely silent for 120 s (the child alarm is the final
    // backstop, and Chaos-style kills drop the connection at once).
    curl_easy_setopt(c, CURLOPT_LOW_SPEED_LIMIT, 1L);
    curl_easy_setopt(c, CURLOPT_LOW_SPEED_TIME, 120L);
    curl_easy_setopt(c, CURLOPT_NOSIGNAL, 1L);

    CURLcode rc = curl_easy_perform(c);
    long http_code = 0;
    curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &http_code);
    curl_slist_free_all(hdrs);
    curl_easy_cleanup(c);

    if (ctx.client_gone) return;  // nobody left to tell
    if (ctx.got_done && rc == CURLE_OK) {
        chat_emit(ctx, json{{"done", true}}.dump());
    } else {
        sh_add_long(&g_sh->errors, 1);
        std::string msg;
        if (rc != CURLE_OK) {
            msg = std::string("upstream stream failed: ") +
                  curl_easy_strerror(rc);
        } else if (http_code != 200) {
            msg = "upstream returned HTTP " + std::to_string(http_code);
        } else {
            msg = "upstream stream ended without completing";
        }
        chat_emit(ctx, json{{"error", msg}}.dump());
    }
}

// ---- /embed: proxy to Lemonade embeddings --------------------------
static void handle_embed(int fd, const Request& req) {
    std::string text;
    try {
        json j = json::parse(req.body);
        if (!j.contains("text") || !j["text"].is_string()) {
            send_error(fd, 400, "Bad Request", "missing string field: text");
            return;
        }
        text = j["text"].get<std::string>();
    } catch (...) {
        send_error(fd, 400, "Bad Request", "request body is not valid JSON");
        return;
    }

    json up{{"model", kEmbedModel}, {"input", text}};
    std::string up_body = up.dump();
    std::string resp;
    CURL* c = curl_easy_init();
    if (!c) {
        send_error(fd, 502, "Bad Gateway", "failed to initialise HTTP client");
        return;
    }
    struct curl_slist* hdrs = nullptr;
    hdrs = curl_slist_append(hdrs, "Content-Type: application/json");
    std::string url = std::string(kLemondBase) + "/api/v1/embeddings";
    curl_easy_setopt(c, CURLOPT_URL, url.c_str());
    curl_easy_setopt(c, CURLOPT_HTTPHEADER, hdrs);
    curl_easy_setopt(c, CURLOPT_POSTFIELDS, up_body.c_str());
    curl_easy_setopt(c, CURLOPT_POSTFIELDSIZE, static_cast<long>(up_body.size()));
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, &collect_cb);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, &resp);
    // "Upstream down" must surface as 502 within 2 s: connects to a
    // dead local port refuse instantly, and the 1 s connect timeout
    // caps every other no-answer case.
    curl_easy_setopt(c, CURLOPT_CONNECTTIMEOUT_MS, 1000L);
    curl_easy_setopt(c, CURLOPT_TIMEOUT_MS, 30000L);
    curl_easy_setopt(c, CURLOPT_NOSIGNAL, 1L);
    CURLcode rc = curl_easy_perform(c);
    long http_code = 0;
    if (rc == CURLE_OK) curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &http_code);
    curl_slist_free_all(hdrs);
    curl_easy_cleanup(c);

    if (rc != CURLE_OK) {
        send_error(fd, 502, "Bad Gateway",
                   std::string("embeddings upstream unreachable: ") +
                       curl_easy_strerror(rc));
        return;
    }
    if (http_code != 200) {
        send_error(fd, 502, "Bad Gateway",
                   "embeddings upstream returned HTTP " +
                       std::to_string(http_code));
        return;
    }
    try {
        json j = json::parse(resp);
        json emb = j.at("data").at(0).at("embedding");
        json out;
        out["dims"] = emb.size();
        out["embedding"] = emb;
        send_json(fd, 200, "OK", out.dump());
    } catch (...) {
        send_error(fd, 502, "Bad Gateway",
                   "embeddings upstream sent an unparseable response");
    }
}

// ---- /transcribe: raw WAV in, text out, via Lemonade Whisper -------
static void handle_transcribe(int fd, const Request& req) {
    if (req.body.empty()) {
        send_error(fd, 400, "Bad Request", "empty body: expected WAV bytes");
        return;
    }
    std::string resp;
    CURL* c = curl_easy_init();
    if (!c) {
        send_error(fd, 502, "Bad Gateway", "failed to initialise HTTP client");
        return;
    }
    curl_mime* mime = curl_mime_init(c);
    curl_mimepart* part = curl_mime_addpart(mime);
    curl_mime_name(part, "model");
    curl_mime_data(part, kSttModel, CURL_ZERO_TERMINATED);
    part = curl_mime_addpart(mime);
    curl_mime_name(part, "file");
    curl_mime_filename(part, "audio.wav");
    curl_mime_type(part, "audio/wav");
    curl_mime_data(part, req.body.data(), req.body.size());

    std::string url = std::string(kLemondBase) + "/api/v1/audio/transcriptions";
    curl_easy_setopt(c, CURLOPT_URL, url.c_str());
    curl_easy_setopt(c, CURLOPT_MIMEPOST, mime);
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, &collect_cb);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, &resp);
    curl_easy_setopt(c, CURLOPT_CONNECTTIMEOUT_MS, 2000L);
    curl_easy_setopt(c, CURLOPT_TIMEOUT_MS, 600000L);  // cold model loads
    curl_easy_setopt(c, CURLOPT_NOSIGNAL, 1L);
    CURLcode rc = curl_easy_perform(c);
    long http_code = 0;
    if (rc == CURLE_OK) curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &http_code);
    curl_mime_free(mime);
    curl_easy_cleanup(c);

    if (rc != CURLE_OK) {
        send_error(fd, 502, "Bad Gateway",
                   std::string("transcription upstream unreachable: ") +
                       curl_easy_strerror(rc));
        return;
    }
    if (http_code != 200) {
        send_error(fd, 502, "Bad Gateway",
                   "transcription upstream returned HTTP " +
                       std::to_string(http_code));
        return;
    }
    try {
        json j = json::parse(resp);
        std::string text = j.at("text").get<std::string>();
        send_json(fd, 200, "OK", json{{"text", text}}.dump());
    } catch (...) {
        send_error(fd, 502, "Bad Gateway",
                   "transcription upstream sent an unparseable response");
    }
}

// Route one connection. Runs entirely inside the handler child; any
// exception escaping a handler is caught here so the child still
// exits through the normal path (the listener never sees it either
// way -- this is belt-and-braces, not the containment mechanism).
static void handle_connection(int fd) {
    struct timeval tv {15, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);

    Request req;
    if (!read_request(fd, req)) return;

    try {
        if (req.method == "GET" && req.path == "/health") {
            handle_health(fd);
        } else if (req.method == "POST" && req.path == "/chat") {
            handle_chat(fd, req);
        } else if (req.method == "POST" && req.path == "/embed") {
            handle_embed(fd, req);
        } else if (req.method == "POST" && req.path == "/transcribe") {
            handle_transcribe(fd, req);
        } else {
            send_error(fd, 404, "Not Found",
                       "no such route: " + req.method + " " + req.path);
        }
    } catch (...) {
        // Last-ditch: try to tell the client, then exit normally.
        try {
            send_error(fd, 500, "Internal Server Error",
                       "handler exception (contained)");
        } catch (...) {}
    }
}

// ------------------------------------------------------------------
// Poller child: probes both upstreams on a ~2 s period and publishes
// state into the shared page.
// ------------------------------------------------------------------
static void poller_main() {
    curl_global_init(CURL_GLOBAL_DEFAULT);
    const std::string lemond_url = std::string(kLemondBase) + "/api/v1/models";
    const std::string flm_url = std::string(kFlmBase) + "/v1/models";
    for (;;) {
        std::time_t t0 = std::time(nullptr);
        sh_set_int(&g_sh->lemond_up, probe_up(lemond_url.c_str()) ? 1 : 0);
        sh_set_int(&g_sh->flm_up, probe_up(flm_url.c_str()) ? 1 : 0);
        long elapsed = static_cast<long>(std::time(nullptr) - t0);
        long rest = 2 - elapsed;
        if (rest > 0) sleep(static_cast<unsigned>(rest));
        else usleep(200000);  // probes were slow (dead upstreams); breathe
    }
}

// ------------------------------------------------------------------
// Listener (parent)
// ------------------------------------------------------------------
static pid_t g_poller_pid = -1;

static void on_sigchld(int) {
    int status;
    pid_t pid;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        if (WIFSIGNALED(status)) {
            // A handler died by signal (segfault/abort/alarm). The
            // listener is untouched; count it and move on. The poller
            // is expected to exit only via our own SIGTERM at shutdown.
            if (pid != g_poller_pid) {
                sh_add_long(&g_sh->worker_crashes, 1);
                sh_add_long(&g_sh->errors, 1);
            }
        }
    }
}

static void on_term(int) {
    if (g_poller_pid > 0) kill(g_poller_pid, SIGTERM);
    _exit(0);
}

int main() {
    std::signal(SIGPIPE, SIG_IGN);
    curl_global_init(CURL_GLOBAL_DEFAULT);

    void* mem = mmap(nullptr, sizeof(Shared), PROT_READ | PROT_WRITE,
                     MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (mem == MAP_FAILED) {
        std::perror("mmap");
        return 1;
    }
    g_sh = static_cast<Shared*>(mem);
    std::memset(g_sh, 0, sizeof(Shared));
    g_sh->start_epoch = static_cast<long>(std::time(nullptr));

    // Reaper + crash counter.
    struct sigaction sa {};
    sa.sa_handler = &on_sigchld;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, nullptr);

    struct sigaction st {};
    st.sa_handler = &on_term;
    sigemptyset(&st.sa_mask);
    sigaction(SIGTERM, &st, nullptr);
    sigaction(SIGINT, &st, nullptr);

    // Upstream poller.
    g_poller_pid = fork();
    if (g_poller_pid == 0) {
        poller_main();
        _exit(0);
    }
    if (g_poller_pid < 0) {
        std::perror("fork (poller)");
        return 1;
    }

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) { std::perror("socket"); return 1; }
    int one = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(kPort);
    if (bind(lfd, reinterpret_cast<sockaddr*>(&addr), sizeof addr) < 0) {
        std::perror("bind");
        return 1;
    }
    if (listen(lfd, 128) < 0) { std::perror("listen"); return 1; }

    std::printf("bridgekeeper-cpp listening on %d\n", kPort);
    std::fflush(stdout);

    for (;;) {
        int cfd = accept(lfd, nullptr, nullptr);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            std::perror("accept");
            usleep(10000);
            continue;
        }
        sh_add_long(&g_sh->requests, 1);
        pid_t pid = fork();
        if (pid == 0) {
            // Handler child. _exit (not exit) so no inherited stdio
            // buffers or atexit handlers run here.
            close(lfd);
            std::signal(SIGCHLD, SIG_DFL);
            alarm(kChildAlarmS);
            handle_connection(cfd);
            close(cfd);
            _exit(0);
        }
        close(cfd);
        if (pid < 0) {
            sh_add_long(&g_sh->errors, 1);
            usleep(50000);  // fork pressure: brief back-off
        }
    }
}
