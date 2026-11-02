defmodule Bridgekeeper.MixProject do
  use Mix.Project

  def project do
    [
      app: :bridgekeeper,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: []
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto],
      mod: {Bridgekeeper.Application, []}
    ]
  end
end
