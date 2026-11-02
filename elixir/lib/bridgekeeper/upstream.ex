defmodule Bridgekeeper.Upstream do
  @moduledoc """
  A minimal HTTP/1.1 client on :gen_tcp -- just enough for the two
  upstream services, in the two shapes they need:

  * `request/6` -- one shot: send a request, read the whole response
    (embed, transcribe, health polls).
  * `open_stream/6` + `next_line/2` -- a buffered line reader over a
    streaming response (FLM chat SSE), decoding chunked transfer
    encoding transparently. A dead or silent upstream surfaces as
    `{:error, reason}` from `next_line`, never as a hang: every read
    carries a timeout, and a closed socket is an immediate error.

  Buffer discipline (the subtle part): the %Reader{} keeps TWO
  buffers -- `raw` holds bytes straight off the socket for the
  de-chunking pump; `decoded` holds de-chunked body bytes awaiting
  line splitting. They must never share a field: chunk framing and
  line framing straddle each other's boundaries arbitrarily.

  Everything is `Connection: close`; one socket per call, no pool.
  For a local bridge fronting two loopback services that is the
  robust shape: no pooled-connection state to go stale when an
  upstream is killed and restarted.
  """

  @connect_timeout 1_500
  # Streaming heads may wait on an upstream model load; one-shot
  # requests use their own recv_timeout for the head instead (for a
  # non-streaming response the head only arrives once the upstream
  # has finished the work -- Whisper needs seconds, not milliseconds).
  @stream_head_timeout 30_000

  defmodule Reader do
    @moduledoc "State for reading one response body."
    defstruct sock: nil,
              raw: "",
              decoded: "",
              # framing: {:length, left} | :chunked | :close
              framing: :close,
              chunk_left: 0,
              # chunk parser state: :chunk_size | :chunk_data | :done
              state: :body
  end

  # ------------------------------------------------------------------
  # One-shot requests
  # ------------------------------------------------------------------

  @doc """
  POST (method "POST"; body may be nil for GET-style) and read the
  full response. Returns {:ok, status, body} | {:error, reason}.
  `recv_timeout` bounds each socket read; callers pick it per
  endpoint (embed must fail fast; transcribe must tolerate a cold
  Whisper load).
  """
  def request(host, port, method, path, body, extra_headers \\ [], recv_timeout \\ 10_000) do
    with {:ok, sock} <- connect(host, port),
         :ok <- send_request(sock, method, path, body, extra_headers),
         {:ok, status, _headers, reader} <- read_head(sock, recv_timeout) do
      result = collect(reader, recv_timeout)
      :gen_tcp.close(sock)

      case result do
        {:ok, resp_body} -> {:ok, status, resp_body}
        {:error, _} = err -> err
      end
    end
  end

  @doc """
  Open a streaming response: sends the request, reads the head, and
  returns {:ok, status, %Reader{}} for use with next_line/2.
  On error the socket is already closed.
  """
  def open_stream(host, port, method, path, body, extra_headers \\ []) do
    with {:ok, sock} <- connect(host, port),
         :ok <- send_request(sock, method, path, body, extra_headers),
         {:ok, status, _headers, reader} <- read_head(sock, @stream_head_timeout) do
      {:ok, status, reader}
    end
  end

  defp connect(host, port) do
    :gen_tcp.connect(host, port, [:binary, packet: :raw, active: false],
      @connect_timeout
    )
  end

  defp send_request(sock, method, path, body, extra_headers) do
    headers = [
      "#{method} #{path} HTTP/1.1\r\n",
      "host: upstream\r\n",
      "connection: close\r\n",
      if(body, do: "content-length: #{byte_size(body)}\r\n", else: ""),
      Enum.map(extra_headers, fn {k, v} -> "#{k}: #{v}\r\n" end),
      "\r\n"
    ]

    :gen_tcp.send(sock, [headers, body || ""])
  end

  # ------------------------------------------------------------------
  # Response head
  # ------------------------------------------------------------------

  defp read_head(sock, timeout) do
    case read_until(sock, "", "\r\n\r\n", timeout, 65_536) do
      {:ok, head, rest} ->
        [status_line | header_lines] = String.split(head, "\r\n")
        headers = parse_headers(header_lines)

        framing =
          cond do
            String.contains?(
              String.downcase(headers["transfer-encoding"] || ""),
              "chunked"
            ) ->
              :chunked

            headers["content-length"] ->
              {:length, String.to_integer(headers["content-length"])}

            true ->
              :close
          end

        reader = %Reader{
          sock: sock,
          raw: rest,
          framing: framing,
          state: if(framing == :chunked, do: :chunk_size, else: :body)
        }

        {:ok, parse_status(status_line), headers, reader}

      {:error, _} = err ->
        :gen_tcp.close(sock)
        err
    end
  end

  defp parse_status(line) do
    case String.split(line, " ") do
      [_, code | _] -> String.to_integer(code)
      _ -> 0
    end
  end

  defp parse_headers(lines) do
    Map.new(lines, fn line ->
      case String.split(line, ":", parts: 2) do
        [k, v] -> {String.downcase(String.trim(k)), String.trim(v)}
        _ -> {"", ""}
      end
    end)
  end

  defp read_until(sock, acc, marker, timeout, max) do
    cond do
      byte_size(acc) > max ->
        {:error, :head_too_large}

      true ->
        case :binary.match(acc, marker) do
          {idx, len} ->
            <<head::binary-size(idx), _::binary-size(len), rest::binary>> = acc
            {:ok, head, rest}

          :nomatch ->
            case :gen_tcp.recv(sock, 0, timeout) do
              {:ok, data} -> read_until(sock, acc <> data, marker, timeout, max)
              {:error, _} = err -> err
            end
        end
    end
  end

  # ------------------------------------------------------------------
  # Byte pump: yields decoded body bytes from the raw buffer,
  # handling all three framings.
  #   {:data, bytes, reader} | {:eof, reader} | {:error, reason}
  # ------------------------------------------------------------------

  defp collect(reader, timeout), do: collect(reader, timeout, [])

  defp collect(reader, timeout, acc) do
    case pump(reader, timeout) do
      {:data, data, reader2} -> collect(reader2, timeout, [data | acc])
      {:eof, _reader2} -> {:ok, acc |> Enum.reverse() |> IO.iodata_to_binary()}
      {:error, _} = err -> err
    end
  end

  defp pump(%Reader{state: :done} = reader, _timeout), do: {:eof, reader}

  defp pump(%Reader{framing: :close, raw: raw, sock: sock} = reader, timeout) do
    if raw != "" do
      {:data, raw, %{reader | raw: ""}}
    else
      case :gen_tcp.recv(sock, 0, timeout) do
        {:ok, data} -> {:data, data, reader}
        {:error, :closed} -> {:eof, %{reader | state: :done}}
        {:error, _} = err -> err
      end
    end
  end

  defp pump(%Reader{framing: {:length, left}, raw: raw, sock: sock} = reader, timeout) do
    cond do
      left == 0 ->
        {:eof, %{reader | state: :done}}

      raw != "" ->
        take = min(byte_size(raw), left)
        <<data::binary-size(take), rest::binary>> = raw
        {:data, data, %{reader | raw: rest, framing: {:length, left - take}}}

      true ->
        case :gen_tcp.recv(sock, 0, timeout) do
          {:ok, data} -> pump(%{reader | raw: data}, timeout)
          {:error, :closed} -> {:error, :unexpected_eof}
          {:error, _} = err -> err
        end
    end
  end

  defp pump(%Reader{framing: :chunked, state: :chunk_size} = reader, timeout) do
    case raw_line(reader, timeout) do
      {:line, line, reader2} ->
        size =
          line
          |> String.split(";", parts: 2)
          |> hd()
          |> String.trim()
          |> Integer.parse(16)
          |> elem(0)

        if size == 0 do
          {:eof, %{reader2 | state: :done}}
        else
          pump(%{reader2 | state: :chunk_data, chunk_left: size}, timeout)
        end

      {:eof, reader2} ->
        {:eof, reader2}

      {:error, _} = err ->
        err
    end
  end

  defp pump(%Reader{framing: :chunked, state: :chunk_data, chunk_left: left, raw: raw, sock: sock} = reader, timeout) do
    cond do
      left == 0 ->
        case raw_take(reader, 2, timeout) do
          {:ok, _crlf, reader2} -> pump(%{reader2 | state: :chunk_size}, timeout)
          {:eof, reader2} -> {:eof, reader2}
          {:error, _} = err -> err
        end

      raw != "" ->
        take = min(byte_size(raw), left)
        <<data::binary-size(take), rest::binary>> = raw
        {:data, data, %{reader | raw: rest, chunk_left: left - take}}

      true ->
        case :gen_tcp.recv(sock, 0, timeout) do
          {:ok, data} -> pump(%{reader | raw: data}, timeout)
          {:error, :closed} -> {:error, :unexpected_eof}
          {:error, _} = err -> err
        end
    end
  end

  # One \n-terminated line from the raw buffer (chunk-size lines).
  defp raw_line(%Reader{raw: raw, sock: sock} = reader, timeout) do
    case :binary.match(raw, "\n") do
      {idx, 1} ->
        <<line::binary-size(idx), _::binary-size(1), rest::binary>> = raw
        {:line, String.trim_trailing(line, "\r"), %{reader | raw: rest}}

      :nomatch ->
        case :gen_tcp.recv(sock, 0, timeout) do
          {:ok, data} -> raw_line(%{reader | raw: raw <> data}, timeout)
          {:error, :closed} -> {:eof, %{reader | state: :done}}
          {:error, _} = err -> err
        end
    end
  end

  # Exactly n bytes from the raw buffer (chunk-terminating CRLF).
  defp raw_take(%Reader{raw: raw} = reader, n, _timeout) when byte_size(raw) >= n do
    <<data::binary-size(n), rest::binary>> = raw
    {:ok, data, %{reader | raw: rest}}
  end

  defp raw_take(%Reader{raw: raw, sock: sock} = reader, n, timeout) do
    case :gen_tcp.recv(sock, 0, timeout) do
      {:ok, data} -> raw_take(%{reader | raw: raw <> data}, n, timeout)
      {:error, :closed} -> {:eof, reader}
      {:error, _} = err -> err
    end
  end

  # ------------------------------------------------------------------
  # Streaming line reader (SSE over the decoded body)
  # ------------------------------------------------------------------

  @doc """
  Next line of a streaming response body:
  {:line, line, reader} | {:eof, reader} | {:error, reason}.
  `timeout` bounds how long the upstream may stay silent before the
  caller treats it as dead -- this is what makes "never hang" a
  property of the code rather than a hope.
  """
  def next_line(%Reader{decoded: decoded} = reader, timeout) do
    case :binary.match(decoded, "\n") do
      {idx, 1} ->
        <<line::binary-size(idx), _::binary-size(1), rest::binary>> = decoded
        {:line, String.trim_trailing(line, "\r"), %{reader | decoded: rest}}

      :nomatch ->
        case pump(reader, timeout) do
          {:data, data, reader2} ->
            next_line(%{reader2 | decoded: decoded <> data}, timeout)

          {:eof, reader2} ->
            if decoded == "" do
              {:eof, reader2}
            else
              # A final unterminated line still counts as a line.
              {:line, String.trim_trailing(decoded, "\r"),
               %{reader2 | decoded: ""}}
            end

          {:error, _} = err ->
            err
        end
    end
  end
end
