defmodule PhoenixChannelFixture.Endpoint do
  use Phoenix.Endpoint, otp_app: :phoenix_channel_fixture

  socket("/socket", PhoenixChannelFixture.UserSocket,
    auth_token: true,
    websocket: [
      check_origin: false,
      connect_info: [:auth_token]
    ],
    longpoll: false
  )
end
