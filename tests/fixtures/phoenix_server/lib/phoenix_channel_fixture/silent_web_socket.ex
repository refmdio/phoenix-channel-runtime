defmodule PhoenixChannelFixture.SilentWebSocketPlug do
  @behaviour Plug

  @impl Plug
  def init(options), do: options

  @impl Plug
  def call(%Plug.Conn{request_path: "/health-fallback/websocket"} = conn, _options) do
    WebSockAdapter.upgrade(conn, PhoenixChannelFixture.SilentWebSocket, nil, [])
  end

  def call(conn, _options), do: conn
end

defmodule PhoenixChannelFixture.SilentWebSocket do
  @behaviour WebSock

  @impl WebSock
  def init(state), do: {:ok, state}

  @impl WebSock
  def handle_in(_frame, state), do: {:ok, state}

  @impl WebSock
  def handle_info(_message, state), do: {:ok, state}
end
