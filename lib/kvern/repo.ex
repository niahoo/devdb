defmodule Kvern.Repo do
  alias Kvern.Repo.Ets

  @m __MODULE__

  defstruct [:mod, :state]

  def new(mod) do
    %@m{mod: Ets, state: Ets.initialize()}
  end

  def put(store = %@m{mod: mod, state: state}, key, value) do
    %{store | state: mod.put(state, key, value)}
  end

  def delete(store = %@m{mod: mod, state: state}, key) do
    %{store | state: mod.delete(state, key)}
  end

  def fetch(%@m{mod: mod, state: state}, key) do
    mod.fetch(state, key)
  end

  def keys(%@m{mod: mod, state: state}) do
    mod.keys(state)
  end

  def get(store, key, default) do
    case fetch(store, key) do
      {:ok, found} ->
        found

      :error ->
        default
    end
  end
end
