defmodule PhoenixChannelFixture.RoomChannel do
  use Phoenix.Channel

  @impl true
  def join("room:" <> room, %{"name" => name}, socket) do
    {:ok, %{name: name, room: room}, assign(socket, :name, name)}
  end

  def join(_topic, _payload, _socket), do: {:error, %{reason: "invalid join"}}

  @impl true
  def handle_in("echo", payload, socket), do: {:reply, {:ok, payload}, socket}

  def handle_in("broadcast", payload, socket) do
    broadcast!(socket, "broadcast", Map.put(payload, "sender", socket.assigns.name))
    {:reply, {:ok, %{"sent" => true}}, socket}
  end
end
