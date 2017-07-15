# A storage is a simple map
defmodule Kvern.Storage do
  import ShorterMaps

  defstruct [kvs: %{}, tainted: []]

  def new, do: %__MODULE__{}

  def kv_put(storage, key, value) do
    syskey = {:kvs, key}
    storage
      |> Map.update!(:kvs, fn map ->
          Map.put(map, key, value)
         end)
      |> taint(syskey)
  end

  defp taint(storage, syskey) do
    storage
      |> Map.update!(:tainted, fn syskeys -> [syskey | syskeys] end)
  end

  def kv_get(storage, key),
    do: Map.get(storage.kvs, key)

  def kv_fetch(storage, key),
    do: Map.fetch(storage.kvs, key)

  def tainted(storage) do
    storage.tainted
    |> Enum.uniq
  end

  def tainted?(%{tainted: []}), do: false
  def tainted?(_), do: true

  def clear_tainted(storage),
    do: Map.put(storage, :tainted, [])

  def all_tainted_data(~M(tainted) = storage) do
    tainted
      |> Enum.reduce(%{}, fn(syskey, acc) ->
          filename = sys_key_filename(syskey)
          Map.put(acc, filename, sys_get(storage, syskey))
         end)
  end

  # get the filename for a stored key. User keys (in :kvs) are simply the user
  # key itself. We may add syskeys like :meta, :sys, :savepoint, stuff like that
  defp sys_key_filename({:kvs, key}), do: key

  # Gets a value on the whole storage : in user space (:kvs) or other
  # information (YAGNI ?)
  defp sys_get(~M(kvs), {:kvs, key}),
    do: Map.fetch!(kvs, key)
end

