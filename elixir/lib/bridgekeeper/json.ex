defmodule Bridgekeeper.JSON do
  @moduledoc """
  A small, complete JSON codec (RFC 8259) on the standard library
  only. The bridge shuffles three JSON dialects (its own contract,
  FLM's SSE chunks, Lemonade's responses); a hand-rolled codec keeps
  the dependency count at zero, which the bake-off scores, and keeps
  every parse failure local and explainable.

  Decode: objects become maps with binary keys, numbers become
  integers or floats, `\\uXXXX` escapes incl. surrogate pairs are
  resolved to UTF-8.
  """

  # ------------------------------------------------------------------
  # Encoding
  # ------------------------------------------------------------------

  def encode!(term), do: IO.iodata_to_binary(enc(term))

  defp enc(nil), do: "null"
  defp enc(true), do: "true"
  defp enc(false), do: "false"
  defp enc(n) when is_integer(n), do: Integer.to_string(n)
  defp enc(n) when is_float(n), do: :erlang.float_to_binary(n, [:short])
  defp enc(s) when is_binary(s), do: [?", esc(s), ?"]

  defp enc(list) when is_list(list) do
    [?[, list |> Enum.map(&enc/1) |> Enum.intersperse(?,), ?]]
  end

  defp enc(map) when is_map(map) do
    pairs =
      Enum.map(map, fn {k, v} -> [enc(to_string(k)), ?:, enc(v)] end)

    [?{, Enum.intersperse(pairs, ?,), ?}]
  end

  defp esc(s) do
    for <<c <- s>> do
      case c do
        ?" -> ~S(\")
        ?\\ -> ~S(\\)
        ?\n -> ~S(\n)
        ?\r -> ~S(\r)
        ?\t -> ~S(\t)
        ?\b -> ~S(\b)
        ?\f -> ~S(\f)
        c when c < 0x20 -> :io_lib.format("\\u~4.16.0b", [c])
        c -> <<c>>
      end
    end
  end

  # ------------------------------------------------------------------
  # Decoding
  # ------------------------------------------------------------------

  @spec decode(binary) :: {:ok, term} | {:error, term}
  def decode(bin) when is_binary(bin) do
    case value(skip_ws(bin)) do
      {:ok, term, rest} ->
        if skip_ws(rest) == "", do: {:ok, term}, else: {:error, :trailing_data}

      {:error, _} = err ->
        err
    end
  end

  defp skip_ws(<<c, rest::binary>>) when c in [?\s, ?\t, ?\n, ?\r],
    do: skip_ws(rest)

  defp skip_ws(bin), do: bin

  defp value(<<"{", rest::binary>>), do: object(skip_ws(rest), %{})
  defp value(<<"[", rest::binary>>), do: array(skip_ws(rest), [])
  defp value(<<"\"", rest::binary>>), do: string(rest, [])
  defp value(<<"true", rest::binary>>), do: {:ok, true, rest}
  defp value(<<"false", rest::binary>>), do: {:ok, false, rest}
  defp value(<<"null", rest::binary>>), do: {:ok, nil, rest}

  defp value(<<c, _::binary>> = bin) when c == ?- or (c >= ?0 and c <= ?9),
    do: number(bin)

  defp value(_), do: {:error, :invalid_value}

  defp object(<<"}", rest::binary>>, acc), do: {:ok, acc, rest}

  defp object(bin, acc) do
    with {:ok, key, rest} <- string_start(bin),
         rest <- skip_ws(rest),
         {:ok, rest} <- expect_colon(rest),
         {:ok, val, rest} <- value(skip_ws(rest)) do
      acc = Map.put(acc, key, val)

      case skip_ws(rest) do
        <<",", rest2::binary>> -> object(skip_ws(rest2), acc)
        <<"}", rest2::binary>> -> {:ok, acc, rest2}
        _ -> {:error, :invalid_object}
      end
    end
  end

  defp string_start(<<"\"", rest::binary>>), do: string(rest, [])
  defp string_start(_), do: {:error, :expected_string}

  defp expect_colon(<<":", rest::binary>>), do: {:ok, rest}
  defp expect_colon(_), do: {:error, :expected_colon}

  defp array(<<"]", rest::binary>>, acc), do: {:ok, Enum.reverse(acc), rest}

  defp array(bin, acc) do
    with {:ok, val, rest} <- value(bin) do
      case skip_ws(rest) do
        <<",", rest2::binary>> -> array(skip_ws(rest2), [val | acc])
        <<"]", rest2::binary>> -> {:ok, Enum.reverse([val | acc]), rest2}
        _ -> {:error, :invalid_array}
      end
    end
  end

  # String body parser; acc is a list of UTF-8 binaries.
  defp string(<<"\"", rest::binary>>, acc),
    do: {:ok, acc |> Enum.reverse() |> IO.iodata_to_binary(), rest}

  defp string(<<"\\\"", rest::binary>>, acc), do: string(rest, ["\"" | acc])
  defp string(<<"\\\\", rest::binary>>, acc), do: string(rest, ["\\" | acc])
  defp string(<<"\\/", rest::binary>>, acc), do: string(rest, ["/" | acc])
  defp string(<<"\\n", rest::binary>>, acc), do: string(rest, ["\n" | acc])
  defp string(<<"\\r", rest::binary>>, acc), do: string(rest, ["\r" | acc])
  defp string(<<"\\t", rest::binary>>, acc), do: string(rest, ["\t" | acc])
  defp string(<<"\\b", rest::binary>>, acc), do: string(rest, ["\b" | acc])
  defp string(<<"\\f", rest::binary>>, acc), do: string(rest, ["\f" | acc])

  defp string(<<"\\u", hex::binary-size(4), rest::binary>>, acc) do
    case Integer.parse(hex, 16) do
      {cp, ""} when cp in 0xD800..0xDBFF ->
        # High surrogate: a low surrogate must follow.
        case rest do
          <<"\\u", hex2::binary-size(4), rest2::binary>> ->
            case Integer.parse(hex2, 16) do
              {lo, ""} when lo in 0xDC00..0xDFFF ->
                full = 0x10000 + (cp - 0xD800) * 0x400 + (lo - 0xDC00)
                string(rest2, [<<full::utf8>> | acc])

              _ ->
                {:error, :bad_surrogate_pair}
            end

          _ ->
            {:error, :bad_surrogate_pair}
        end

      {cp, ""} ->
        string(rest, [<<cp::utf8>> | acc])

      _ ->
        {:error, :bad_unicode_escape}
    end
  end

  defp string(<<c::utf8, rest::binary>>, acc) when c >= 0x20,
    do: string(rest, [<<c::utf8>> | acc])

  defp string(_, _), do: {:error, :unterminated_string}

  @num_re ~r/^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/
  defp number(bin) do
    case Regex.run(@num_re, bin) do
      [tok] ->
        rest = binary_part(bin, byte_size(tok), byte_size(bin) - byte_size(tok))

        val =
          if String.contains?(tok, [".", "e", "E"]) do
            case Float.parse(tok) do
              {f, ""} -> f
              _ -> tok
            end
          else
            case Integer.parse(tok) do
              {i, ""} -> i
              _ -> tok
            end
          end

        if is_number(val), do: {:ok, val, rest}, else: {:error, :bad_number}

      _ ->
        {:error, :bad_number}
    end
  end
end
