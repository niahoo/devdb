defmodule Kvern.Mixfile do
  use Mix.Project

  def project do
    [
      app: :kvern,
      description: """
      This package implements a simple key/value store backed by human readable
      disk files.
      """,
      version: "0.1.0",
      elixir: "~> 1.4",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "Kvern",
      package: package()
    ]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    # Specify extra applications you'll use from Erlang/Elixir
    [extra_applications: [:logger], mod: {Kvern.Application, []}]
  end

  # Dependencies can be Hex packages:
  #
  #   {:my_dep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:my_dep, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
  #
  # Type "mix help deps" for more examples and options
  defp deps do
    [
      # dev
      {:decompilerl, github: "niahoo/decompilerl", only: :dev},
      {:ex_doc, "~> 0.14", only: :dev, runtime: false},
      {:todo, "~> 1.3", runtime: false},

      # runtime
      # {:xdn, path: "../xdn"},
      {:shorter_maps, "~> 2.1"},
      {:unsafe, "~> 1.0"},
      {:gen_loop, "~> 0.1.0"},
      {:plain_fsm, github: "uwiger/plain_fsm", commit: "ae9eca8a8df8f61a32185b06882a55d60e62e904"}
    ]
  end

  defp package() do
    [
      licenses: ["MIT"],
      maintainers: ["niahoo osef <dev@ooha.in>"],
      links: %{"Github" => "https://github.com/niahoo/kvern"}
    ]
  end
end
