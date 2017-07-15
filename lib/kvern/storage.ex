# A storage is a simple map
defmodule Kvern.Storage do

  @prefix_usr :usr

  defstruct [kvs: %{}, tainted: []]

  def new, do: %__MODULE__{}

  def kv_put(storage, key, value) do
    storage
      |> Map.update!(:kvs, fn map ->
          Map.put(map, key, value)
         end)
      |> taint(key)
  end

  defp taint(storage, key) do
    storage
      |> Map.update!(:tainted, fn keys -> [key|keys] end)
  end

  def kv_get(storage, key),
    do: Map.get(storage.kvs, key)

  def kv_fetch(storage, key),
    do: Map.fetch(storage.kvs, key)

  def tainted(storage) do
    storage.tainted
    |> Enum.uniq
  end
end
