defmodule Kvern.Seed do
  @callback init(opts :: [any()]) :: any()
  @callback stream_updates(state :: any()) :: Kvern.t_update()

  @m __MODULE__

  defstruct mod: nil, state: nil

  def new(mod, opts \\ []) do
    %@m{mod: mod, state: mod.init(opts)}
  end

  def stream_updates(%__MODULE__{mod: mod, state: state}) do
    mod.stream_updates(state)
  end
end
