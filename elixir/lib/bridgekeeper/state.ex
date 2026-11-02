defmodule Bridgekeeper.State do
  @moduledoc """
  Shared bridge state: request/error counters and upstream health.

  Storage is chosen so that NO reader ever blocks on the poller:
  counters live in a :counters array (atomic, lock-free), upstream
  states in a public ETS table. This GenServer's only job is to run
  the poll loop; the polls themselves run in spawned tasks so a slow
  or hung upstream can never stall the loop or a /health read.
  """
  use GenServer

  alias Bridgekeeper.Upstream

  @table :bk_state
  @poll_ms 2_000
  @poll_timeout 1_500

  # Counter slots
  @requests 1
  @errors 2

  # ------------------------------------------------------------------
  # Public API (no GenServer calls -- direct atomic reads)
  # ------------------------------------------------------------------

  def start_counters do
    ref = :counters.new(2, [])
    :persistent_term.put({__MODULE__, :counters}, ref)
    ref
  end

  def bump_requests, do: :counters.add(counters(), @requests, 1)
  def bump_errors, do: :counters.add(counters(), @errors, 1)

  def health do
    %{
      "ok" => true,
      "uptime_s" => uptime_s(),
      "lemond" => state_of(:lemond),
      "flm" => state_of(:flm),
      "requests" => :counters.get(counters(), @requests),
      "errors" => :counters.get(counters(), @errors)
    }
  end

  defp counters, do: :persistent_term.get({__MODULE__, :counters})

  defp uptime_s do
    case :ets.lookup(@table, :started_monotonic_s) do
      [{_, t0}] -> System.monotonic_time(:second) - t0
      [] -> 0
    end
  end

  defp state_of(key) do
    case :ets.lookup(@table, key) do
      [{_, :up}] -> "up"
      _ -> "down"
    end
  end

  # ------------------------------------------------------------------
  # GenServer: the poll loop
  # ------------------------------------------------------------------

  def start_link(_), do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

  @impl true
  def init(_) do
    :ets.new(@table, [:named_table, :public, read_concurrency: true])
    :ets.insert(@table, {:started_monotonic_s, System.monotonic_time(:second)})
    :ets.insert(@table, {:lemond, :down})
    :ets.insert(@table, {:flm, :down})
    poll_now()
    schedule()
    {:ok, nil}
  end

  @impl true
  def handle_info(:poll, state) do
    poll_now()
    schedule()
    {:noreply, state}
  end

  def handle_info(_, state), do: {:noreply, state}

  defp schedule, do: Process.send_after(self(), :poll, @poll_ms)

  defp poll_now do
    poll(:lemond, ~c"127.0.0.1", 13_305, "/api/v1/models")
    poll(:flm, ~c"127.0.0.1", 53_999, "/v1/models")
  end

  # Each poll runs in its own unlinked process: worst case it dies or
  # times out alone, and the ETS row simply stays what it was until
  # the next round writes a fresh verdict.
  defp poll(key, host, port, path) do
    spawn(fn ->
      verdict =
        case Upstream.request(host, port, "GET", path, nil, [], @poll_timeout) do
          {:ok, 200, _} -> :up
          _ -> :down
        end

      :ets.insert(@table, {key, verdict})
    end)
  end
end
