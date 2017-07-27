use Mix.Config

config :logger,
  handle_otp_reports: false,
  handle_sasl_reports: false

config :logger, :console,
  # level: :info,
  metadata: [:function, :pid]
