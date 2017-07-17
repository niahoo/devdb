defmodule Kvern.Mixfile do
  use Mix.Project

  def project do
    [app: :kvern,
     version: "0.1.0",
     elixir: "~> 1.4",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    # Specify extra applications you'll use from Erlang/Elixir
    [extra_applications: [:logger],
     mod: {Kvern.Application, []}]
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
      {:plain_fsm_ex, github: "ashneyderman/plain_fsm_ex", branch: "master"},
      {:plain_fsm, github: "uwiger/plain_fsm", override: true},
      {:shorter_maps, "~> 2.1"},
      {:xdn, path: "../xdn"},
      {:mutex, "~> 1.0.0", only: :test},
      {:decompilerl, github: "niahoo/decompilerl"},
      {:poison, "~> 3.1"},
    ]
  end
end
