defmodule Bridgekeeper.Application do
  @moduledoc """
  Supervision tree -- the point of the BEAM entry:

      Bridgekeeper.Supervisor (one_for_one)
      |- Bridgekeeper.State        (counters + upstream poller)
      |- Task.Supervisor           (one supervised task per client
      |                             connection; a handler crash is a
      |                             task exit, nothing more)
      '- Bridgekeeper.Listener     (owns the listen socket; runs the
                                    acceptor loop)

  Handlers are started under the Task.Supervisor with restart:
  :temporary -- a crashed handler is counted and gone, never
  restarted (its client is gone too), and crucially it can never
  take the listener, the poller, or another handler with it.
  """
  use Application

  @impl true
  def start(_type, _args) do
    Bridgekeeper.State.start_counters()

    children = [
      Bridgekeeper.State,
      {Task.Supervisor, name: Bridgekeeper.HandlerSup},
      Bridgekeeper.Listener
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Bridgekeeper.Supervisor)
  end
end
