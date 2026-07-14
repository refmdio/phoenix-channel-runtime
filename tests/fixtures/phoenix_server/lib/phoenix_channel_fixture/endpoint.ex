defmodule PhoenixChannelFixture.Endpoint do
  use Phoenix.Endpoint, otp_app: :phoenix_channel_fixture

  if Version.match?(to_string(Application.spec(:phoenix, :vsn)), ">= 1.8.0") do
    socket("/socket", PhoenixChannelFixture.UserSocket,
      auth_token: true,
      websocket: [
        check_origin: false,
        connect_info: [:auth_token]
      ],
      longpoll: [check_origin: false]
    )

    socket("/health-fallback", PhoenixChannelFixture.UserSocket,
      auth_token: true,
      websocket: false,
      longpoll: [check_origin: false]
    )
  else
    socket("/socket", PhoenixChannelFixture.UserSocket,
      websocket: [check_origin: false],
      longpoll: [check_origin: false]
    )

    socket("/health-fallback", PhoenixChannelFixture.UserSocket,
      websocket: false,
      longpoll: [check_origin: false]
    )
  end

  plug(PhoenixChannelFixture.SilentWebSocketPlug)
end
