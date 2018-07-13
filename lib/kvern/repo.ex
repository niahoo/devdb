defmodule Kvern.Repo do
  alias Kvern.Repo.Ets

  @m __MODULE__

  defstruct mod: nil, state: nil, read_fallback: nil

  def new(mod, opts \\ []) do
    %@m{mod: mod, state: mod.new(), read_fallback: opts[:read_fallback]}
  end

  def put(repo = %@m{mod: mod, state: state}, key, value) do
    %{repo | state: mod.put(state, key, value)}
  end

  def delete(repo = %@m{mod: mod, state: state}, key) do
    %{repo | state: mod.delete(state, key)}
  end

  def nuke(repo = %@m{mod: mod, state: state}) do
    %{repo | state: mod.nuke(state)}
  end

  # Here if we use the fallback, as we do not return the repository from
  # fetch(), we cannot put the fallback value in the repo.
  def fetch(%@m{mod: mod, state: state, read_fallback: read_fallback}, key) do
    case {mod.fetch(state, key), read_fallback} do
      {{:ok, found}, _} ->
        {:ok, found}

      # no fallback to get the data
      {:error, nil} ->
        :error

      {:error, %@m{} = fallback} ->
        fetch(fallback, key)
    end
  end

  def fetch!(%@m{mod: mod, state: state}, key) do
    unwrap_fetch(mod.fetch(state, key), mod, key)
  end

  def unwrap_fetch({:ok, val}, _, _), do: val
  def unwrap_fetch(:error, mod, key), do: raise(KeyError, key: key, term: {__MODULE__, mod})

  def keys(%@m{mod: mod, state: state}) do
    mod.keys(state)
  end

  def get(repo, key, default) do
    case fetch(repo, key) do
      {:ok, found} ->
        found

      :error ->
        default
    end
  end

  def apply_updates(repo, updates), do: Enum.reduce(updates, repo, &apply_up/2)

  def apply_up({:put, key, val}, repo), do: @m.put(repo, key, val)
  def apply_up({:delete, key}, repo), do: @m.delete(repo, key)
end
