defmodule Kvern.Repo.Multi do
  @behaviour Kvern.Repo
  use TODO
  alias Kvern.Repo

  def new(configs) do
    repos = Enum.map(configs, &build_replicate/1)
    repos
  end

  # -- Modifiers functions
  ## We relay the write operations on all repositories

  def put(repos, key, value) do
    repos
    |> Enum.map(&Repo.put(&1, key, value))
  end

  def delete(repos, key) do
    repos
    |> Enum.map(&Repo.delete(&1, key))
  end

  def nuke(repos) do
    repos
    |> Enum.map(&Repo.nuke/1)
  end

  # -- Getter functions
  ## We try to find value from the first repository only, the Multi repo is not
  ## a fallback mechanism.

  @todo """
  Opt-in fallback mechanism with giving :next as read_fallback for Repo.Multi
  instead of giving an actual repo.
  """
  def fetch([first | _], key), do: Repo.fetch(first, key)
  def keys([first | _]), do: Repo.keys(first)

  def transactional(_), do: {:error, :unsupported}

  # --

  defp build_replicate({mod, opts}) when is_atom(mod) and is_list(opts), do: Repo.new(mod, opts)
  defp build_replicate(mod) when is_atom(mod), do: Repo.new(mod)
end
