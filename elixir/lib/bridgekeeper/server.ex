defmodule Bridgekeeper.Listener do
  @moduledoc """
  Owns the listen socket on 127.0.0.1:14111 and the acceptor loop.

  The acceptor runs in a process linked to this GenServer: if the
  acceptor ever dies, the listener dies with it and the supervisor
  restarts both with a fresh socket. Accepted sockets are handed to
  per-connection tasks under the Task.Supervisor, so handler crashes
  are isolated by construction.
  """
  use GenServer

  @port 14_111

  def start_link(_), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @impl true
  def init(_) do
    {:ok, listen} =
      :gen_tcp.listen(@port, [
        :binary,
        packet: :http,
        active: false,
        reuseaddr: true,
        backlog: 256,
        ip: {127, 0, 0, 1}
      ])

    acceptor = spawn_link(fn -> accept_loop(listen) end)
    {:ok, %{listen: listen, acceptor: acceptor}}
  end

  defp accept_loop(listen) do
    case :gen_tcp.accept(listen) do
      {:ok, client} ->
        case Task.Supervisor.start_child(Bridgekeeper.HandlerSup, fn ->
               Bridgekeeper.Handler.handle(client)
             end) do
          {:ok, pid} ->
            # The acceptor owns the socket until it hands it over;
            # without the transfer the socket dies with the acceptor
            # if the acceptor is ever restarted.
            case :gen_tcp.controlling_process(client, pid) do
              :ok -> :ok
              {:error, _} -> :gen_tcp.close(client)
            end

          {:error, _} ->
            :gen_tcp.close(client)
        end

        accept_loop(listen)

      {:error, :closed} ->
        # Listen socket is gone; exit and let supervision rebuild.
        exit(:listen_closed)

      {:error, :emfile} ->
        # Out of file descriptors: back off briefly, keep accepting.
        Process.sleep(100)
        accept_loop(listen)

      {:error, _reason} ->
        Process.sleep(10)
        accept_loop(listen)
    end
  end
end

defmodule Bridgekeeper.Handler do
  @moduledoc """
  One connection = one task = one request. Everything here is
  written to fail locally: any exception is caught at the boundary,
  counted, and turned into a best-effort 500; the socket is always
  closed; nothing here is linked to the listener.
  """

  alias Bridgekeeper.{JSON, State, Upstream}

  @flm_host ~c"127.0.0.1"
  @flm_port 53_999
  @lemond_host ~c"127.0.0.1"
  @lemond_port 13_305
  @flm_model "llama3.2:1b"
  @embed_model "nomic-embed-text-v1-GGUF"
  @stt_model "Whisper-Large-v3-Turbo"
  # An upstream that says nothing for this long mid-stream is dead
  # for practical purposes; the contract is "never hang".
  @stream_idle_timeout 120_000
  # Embed must fail fast when lemond is down: connection refused is
  # instant, and a live-but-stalled lemond gets this per-read budget.
  @embed_timeout 1_500
  # Whisper cold loads measured ~32 s on the reference machine.
  @transcribe_timeout 180_000
  @max_body 32 * 1024 * 1024

  # ------------------------------------------------------------------

  def handle(sock) do
    try do
      case read_request(sock) do
        {:ok, method, path, body} ->
          State.bump_requests()
          route(sock, method, path, body)

        {:error, _} ->
          :ok
      end
    rescue
      _ ->
        State.bump_errors()
        send_json(sock, 500, %{"error" => "internal error"})
    catch
      _, _ ->
        State.bump_errors()
        send_json(sock, 500, %{"error" => "internal error"})
    after
      :gen_tcp.close(sock)
    end
  end

  # ------------------------------------------------------------------
  # Request parsing (packet: :http for the head, raw for the body)
  # ------------------------------------------------------------------

  defp read_request(sock) do
    with {:ok, {:http_request, method, {:abs_path, path}, _ver}} <-
           :gen_tcp.recv(sock, 0, 15_000),
         {:ok, headers} <- read_headers(sock, %{}),
         :ok <- :inet.setopts(sock, packet: :raw),
         {:ok, body} <- read_body(sock, content_length(headers["content-length"])) do
      # packet: :http hands back the path as a charlist or a binary
      # depending on socket mode; to_string/1 normalizes both (and
      # is the identity on binaries).
      {:ok, to_string(method), to_string(path), body}
    end
  end

  defp read_headers(sock, acc) do
    case :gen_tcp.recv(sock, 0, 15_000) do
      {:ok, :http_eoh} ->
        {:ok, acc}

      {:ok, {:http_header, _, name, _, value}} ->
        key = name |> to_string() |> String.downcase()
        val = if is_integer(value), do: value, else: to_string(value)
        read_headers(sock, Map.put(acc, key, val))

      {:error, _} = err ->
        err
    end
  end

  # packet: :http delivers header values in whatever shape Erlang
  # prefers -- Content-Length has been seen as an integer AND as a
  # charlist (which read_headers stringifies). Normalize here; the
  # first version of this code let a string length fall through to
  # the :body_too_large clause and silently dropped every POST.
  defp content_length(nil), do: 0
  defp content_length(v) when is_integer(v), do: v

  defp content_length(v) when is_binary(v) do
    case Integer.parse(String.trim(v)) do
      {n, _} -> n
      :error -> 0
    end
  end

  defp content_length(_), do: 0

  defp read_body(_sock, 0), do: {:ok, ""}

  defp read_body(sock, len) when is_integer(len) and len <= @max_body do
    read_body_acc(sock, len, "")
  end

  defp read_body(_, _), do: {:error, :body_too_large}

  defp read_body_acc(_sock, 0, acc), do: {:ok, acc}

  defp read_body_acc(sock, left, acc) do
    case :gen_tcp.recv(sock, left, 30_000) do
      {:ok, data} -> read_body_acc(sock, left - byte_size(data), acc <> data)
      {:error, _} = err -> err
    end
  end

  # ------------------------------------------------------------------
  # Routes
  # ------------------------------------------------------------------

  defp route(sock, "GET", "/health", _body) do
    send_json(sock, 200, State.health())
  end

  defp route(sock, "POST", "/chat", body), do: chat(sock, body)
  defp route(sock, "POST", "/embed", body), do: embed(sock, body)
  defp route(sock, "POST", "/transcribe", body), do: transcribe(sock, body)

  defp route(sock, _method, _path, _body) do
    State.bump_errors()
    send_json(sock, 404, %{"error" => "not found"})
  end

  # ------------------------------------------------------------------
  # POST /chat -- SSE proxy to FLM
  # ------------------------------------------------------------------

  defp chat(sock, body) do
    case JSON.decode(body) do
      {:ok, %{"prompt" => prompt} = req} when is_binary(prompt) ->
        max_tokens = if is_integer(req["max_tokens"]), do: req["max_tokens"], else: 128
        stream_chat(sock, prompt, max_tokens)

      _ ->
        State.bump_errors()
        send_json(sock, 400, %{"error" => "expected JSON body {\"prompt\": \"...\"}"})
    end
  end

  defp stream_chat(sock, prompt, max_tokens) do
    # The client asked for a stream, so it gets one: headers go out
    # first, and every later failure -- connect refused, bad status,
    # death mid-stream -- is reported IN the stream as an error
    # event, then the stream closes. The one shape that is never
    # produced is a silent hang.
    :gen_tcp.send(sock, [
      "HTTP/1.1 200 OK\r\n",
      "content-type: text/event-stream\r\n",
      "cache-control: no-cache\r\n",
      "connection: close\r\n\r\n"
    ])

    payload =
      JSON.encode!(%{
        "model" => @flm_model,
        "messages" => [%{"role" => "user", "content" => prompt}],
        "max_tokens" => max_tokens,
        "stream" => true
      })

    case Upstream.open_stream(@flm_host, @flm_port, "POST", "/v1/chat/completions",
           payload, [{"content-type", "application/json"}]) do
      {:ok, 200, reader} ->
        result = forward_stream(sock, reader)
        :gen_tcp.close(reader.sock)
        result

      {:ok, status, reader} ->
        :gen_tcp.close(reader.sock)
        sse_error(sock, "flm returned HTTP #{status}")

      {:error, reason} ->
        sse_error(sock, "flm unreachable: #{format_reason(reason)}")
    end
  end

  defp forward_stream(sock, reader) do
    case Upstream.next_line(reader, @stream_idle_timeout) do
      {:line, line, reader2} ->
        case sse_payload(line) do
          :done ->
            send_event(sock, %{"done" => true})

          {:delta, text} ->
            case send_event(sock, %{"delta" => text}) do
              :ok -> forward_stream(sock, reader2)
              {:error, _} -> :client_gone
            end

          :ignore ->
            forward_stream(sock, reader2)
        end

      {:eof, _reader} ->
        sse_error(sock, "flm closed the stream mid-generation")

      {:error, reason} ->
        sse_error(sock, "flm stream error: #{format_reason(reason)}")
    end
  end

  # Extract the payload of an SSE line worth forwarding.
  defp sse_payload("data:" <> rest) do
    data = String.trim(rest)

    cond do
      data == "[DONE]" ->
        :done

      data == "" ->
        :ignore

      true ->
        case JSON.decode(data) do
          {:ok, %{"choices" => [%{"delta" => %{"content" => c}} | _]}}
          when is_binary(c) and c != "" ->
            {:delta, c}

          _ ->
            :ignore
        end
    end
  end

  defp sse_payload(_), do: :ignore

  defp send_event(sock, term) do
    :gen_tcp.send(sock, ["data: ", JSON.encode!(term), "\n\n"])
  end

  defp sse_error(sock, message) do
    State.bump_errors()
    send_event(sock, %{"error" => message})
    :error
  end

  # ------------------------------------------------------------------
  # POST /embed -- proxy to Lemonade embeddings
  # ------------------------------------------------------------------

  defp embed(sock, body) do
    case JSON.decode(body) do
      {:ok, %{"text" => text}} when is_binary(text) ->
        payload = JSON.encode!(%{"model" => @embed_model, "input" => text})

        case Upstream.request(@lemond_host, @lemond_port, "POST",
               "/api/v1/embeddings", payload,
               [{"content-type", "application/json"}], @embed_timeout) do
          {:ok, 200, resp} -> send_embedding(sock, resp)
          {:ok, status, _} -> embed_502(sock, "lemond embeddings returned HTTP #{status}")
          {:error, reason} -> embed_502(sock, "lemond unreachable: #{format_reason(reason)}")
        end

      _ ->
        State.bump_errors()
        send_json(sock, 400, %{"error" => "expected JSON body {\"text\": \"...\"}"})
    end
  end

  defp embed_502(sock, message) do
    State.bump_errors()
    send_json(sock, 502, %{"error" => message})
  end

  # The embedding array is spliced VERBATIM out of the upstream body
  # (768 floats re-encoded would cost time and precision for zero
  # benefit); fall back to a full decode if the splice fails.
  defp send_embedding(sock, resp) do
    case extract_array(resp, "\"embedding\"") do
      {:ok, array_text, dims} ->
        body = "{\"dims\":#{dims},\"embedding\":" <> array_text <> "}"
        send_raw_json(sock, 200, body)

      :error ->
        case JSON.decode(resp) do
          {:ok, %{"data" => [%{"embedding" => list} | _]}} when is_list(list) ->
            send_json(sock, 200, %{"dims" => length(list), "embedding" => list})

          _ ->
            embed_502(sock, "could not parse lemond embeddings response")
        end
    end
  end

  # Find `"key"` then the '[' after it, and scan to the matching ']'.
  # Elements of an embedding array are scalars (no strings, no
  # nesting), so depth counting is exact.
  defp extract_array(resp, key) do
    with {kidx, klen} <- :binary.match(resp, key),
         after_key <- binary_part(resp, kidx + klen, byte_size(resp) - kidx - klen),
         {aidx, 1} <- :binary.match(after_key, "[") do
      scan_array(after_key, aidx, 0, 0, 0)
    else
      _ -> :error
    end
  end

  # scan_array(bin, pos, depth, commas_at_depth_1, start) walks from
  # the '[' at aidx. Elements counted via top-level commas.
  defp scan_array(bin, pos, depth, commas, start) do
    case bin |> binary_part(pos, 1) do
      "[" ->
        scan_array(bin, pos + 1, depth + 1, commas, if(depth == 0, do: pos, else: start))

      "]" ->
        if depth == 1 do
          len = pos - start + 1
          text = binary_part(bin, start, len)
          dims = if len <= 2, do: 0, else: commas + 1
          {:ok, text, dims}
        else
          scan_array(bin, pos + 1, depth - 1, commas, start)
        end

      "," ->
        scan_array(bin, pos + 1, depth, commas + if(depth == 1, do: 1, else: 0), start)

      _ ->
        scan_array(bin, pos + 1, depth, commas, start)
    end
  rescue
    _ -> :error
  end

  # ------------------------------------------------------------------
  # POST /transcribe -- raw WAV body -> Lemonade Whisper (multipart)
  # ------------------------------------------------------------------

  defp transcribe(sock, body) do
    if body == "" do
      State.bump_errors()
      send_json(sock, 400, %{"error" => "expected raw WAV bytes as the body"})
    else
      boundary = "----bk" <> Base.encode16(:crypto.strong_rand_bytes(12), case: :lower)

      multipart =
        IO.iodata_to_binary([
          "--", boundary, "\r\n",
          "content-disposition: form-data; name=\"model\"\r\n\r\n",
          @stt_model, "\r\n",
          "--", boundary, "\r\n",
          "content-disposition: form-data; name=\"file\"; filename=\"audio.wav\"\r\n",
          "content-type: application/octet-stream\r\n\r\n",
          body, "\r\n",
          "--", boundary, "--\r\n"
        ])

      case Upstream.request(@lemond_host, @lemond_port, "POST",
             "/api/v1/audio/transcriptions", multipart,
             [{"content-type", "multipart/form-data; boundary=#{boundary}"}],
             @transcribe_timeout) do
        {:ok, 200, resp} ->
          case JSON.decode(resp) do
            {:ok, %{"text" => text}} ->
              send_json(sock, 200, %{"text" => text})

            _ ->
              State.bump_errors()
              send_json(sock, 502, %{"error" => "could not parse lemond transcription"})
          end

        {:ok, status, _} ->
          State.bump_errors()
          send_json(sock, 502, %{"error" => "lemond transcription returned HTTP #{status}"})

        {:error, reason} ->
          State.bump_errors()
          send_json(sock, 502, %{"error" => "lemond unreachable: #{format_reason(reason)}"})
      end
    end
  end

  # ------------------------------------------------------------------
  # Response helpers
  # ------------------------------------------------------------------

  defp send_json(sock, status, term), do: send_raw_json(sock, status, JSON.encode!(term))

  defp send_raw_json(sock, status, body) do
    :gen_tcp.send(sock, [
      "HTTP/1.1 #{status} #{reason_phrase(status)}\r\n",
      "content-type: application/json\r\n",
      "content-length: #{byte_size(body)}\r\n",
      "connection: close\r\n\r\n",
      body
    ])
  end

  defp reason_phrase(200), do: "OK"
  defp reason_phrase(400), do: "Bad Request"
  defp reason_phrase(404), do: "Not Found"
  defp reason_phrase(500), do: "Internal Server Error"
  defp reason_phrase(502), do: "Bad Gateway"
  defp reason_phrase(_), do: "Status"

  defp format_reason(:timeout), do: "timeout"
  defp format_reason(:closed), do: "connection closed"
  defp format_reason(:econnrefused), do: "connection refused"
  defp format_reason(:unexpected_eof), do: "unexpected end of response"
  defp format_reason(other), do: inspect(other)
end
