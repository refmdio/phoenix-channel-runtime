defmodule PhoenixChannelFixture.MixProject do
  use Mix.Project

  def project do
    [
      app: :phoenix_channel_fixture,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      mod: {PhoenixChannelFixture.Application, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:bandit, "~> 1.7"},
      {:jason, "~> 1.4"},
      {:phoenix, System.get_env("PHOENIX_VERSION", "~> 1.8.0")}
    ]
  end
end
