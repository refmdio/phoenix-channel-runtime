import Config

port = String.to_integer(System.get_env("PORT", "4055"))

config :logger, level: :warning

config :phoenix_channel_fixture, PhoenixChannelFixture.Endpoint,
  adapter: Bandit.PhoenixAdapter,
  http: [ip: {127, 0, 0, 1}, port: port],
  pubsub_server: PhoenixChannelFixture.PubSub,
  secret_key_base: String.duplicate("phoenix-channel-runtime-e2e-", 3),
  server: true,
  url: [host: "localhost"]
