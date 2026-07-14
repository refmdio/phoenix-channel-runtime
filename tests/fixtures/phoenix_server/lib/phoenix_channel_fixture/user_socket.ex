defmodule PhoenixChannelFixture.UserSocket do
  use Phoenix.Socket

  channel("room:*", PhoenixChannelFixture.RoomChannel)

  @impl true
  def connect(%{"client" => "rust"}, socket, %{auth_token: "secret"}), do: {:ok, socket}

  def connect(%{"client" => "rust", "token" => "secret"}, socket, _connect_info),
    do: {:ok, socket}

  def connect(_params, _socket, _connect_info), do: :error

  @impl true
  def id(_socket), do: nil
end
