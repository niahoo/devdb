defmodule Kvern.Repo.Map do
  use TODO
  @m __MODULE__
  @behaviour Kvern.Repo

  defstruct [:data]

  def new(_), do: %@m{data: %{}}

  def put(this, key, value) do
    Map.put(this, :data, Map.put(this.data, key, value))
  end

  def delete(this, key) do
    Map.put(this, :data, Map.delete(this.data, key))
  end

  def fetch(this, key) do
    Map.fetch(this.data, key)
  end

  def keys(this) do
    Map.keys(this.data)
  end

  def nuke(_this) do
    new([])
  end

  def transactional(_), do: {:ok, {__MODULE__, []}}
end
