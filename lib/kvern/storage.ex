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
          external = syskey_to_external(syskey)
          Map.put(acc, external, sys_get(storage, syskey))
         end)
  end

  # get the external for a stored key. User keys (in :kvs) are simply the user
  # key itself, that obey to Kvern keys rules.

  # We may add syskeys like :meta, :sys, :savepoint, stuff like that, with
  # different rules (e.g. start by a dot)
  defp syskey_to_external({:kvs, key}), do: key

  # Reverse function. atm only user keys are handled so they belong to :kvs
  defp external_to_syskey(key), do: {:kvs, key}

  # Creates a fresh storage from data. Keys are evaluated
  def sys_import(data) do
    empty = new()
    Enum.reduce(data, empty, fn({k, v}, storage) ->
      sys_import(storage, k, v)
    end)
  end

  def sys_import(storage, extkey, value) do
    key = external_to_syskey(extkey)
    sys_put(storage, key, value)
  end

  # Gets a value on the whole storage : in user space (:kvs) or other
  # information (YAGNI ?)
  defp sys_get(~M(kvs), {:kvs, key}),
    do: Map.fetch!(kvs, key)


  # Sets a value on the whole storage : in user space (:kvs) or other
  # information (YAGNI ?)
  def sys_put(storage, {:kvs, key}, value),
    do: kv_put(storage, key, value)

end

