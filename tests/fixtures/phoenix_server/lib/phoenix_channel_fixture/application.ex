defmodule PhoenixChannelFixture.Application do
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Phoenix.PubSub, name: PhoenixChannelFixture.PubSub},
      PhoenixChannelFixture.Endpoint
    ]

    Supervisor.start_link(children,
      strategy: :one_for_one,
      name: PhoenixChannelFixture.Supervisor
    )
  end
end
