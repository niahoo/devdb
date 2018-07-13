defmodule Kvern.Repo.Transactional do
  @m __MODULE__

  defstruct [:data]

  def new(), do: %@m{data: %{}}

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
end
