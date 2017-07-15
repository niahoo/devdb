defmodule Kvern.Application do
  # See http://elixir-lang.org/docs/stable/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    import Supervisor.Spec
    database_supervisor_spec = [
      [worker(Kvern.Store, [], restart: :transient)],
      [strategy: :simple_one_for_one, name: Kvern.StoreSupervisor]
    ]

    # Define workers and child supervisors to be supervised
    children = [
      # Starts a worker by calling: Kvern.Worker.start_link(arg1, arg2, arg3)
      supervisor(Registry, [:unique, Kvern.Registry]),
      supervisor(Supervisor,  database_supervisor_spec),
    ]

    # See http://elixir-lang.org/docs/stable/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :rest_for_one, name: Kvern.MainSupervisor]
    Supervisor.start_link(children, opts)
  end
end
