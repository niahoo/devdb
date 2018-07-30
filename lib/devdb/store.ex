defprotocol DevDB.Store.Protocol do
  def put_entry(state, entry)
  def delete_entry(state, key)
  def fetch_entry(state, key)
  def reduce_entries(state, acc, fun)
  def reduce_tr_entries(state, ref, acc, fun)
end

defmodule DevDB.Store do
  def each_entries(state, fun) do
    reduce_entries(state, nil, fn entry, acc ->
      fun.(entry)
    end)
  end

  defdelegate put_entry(state, entry), to: __MODULE__.Protocol
  defdelegate delete_entry(state, key), to: __MODULE__.Protocol
  defdelegate fetch_entry(state, key), to: __MODULE__.Protocol
  defdelegate reduce_entries(state, acc, fun), to: __MODULE__.Protocol
  defdelegate reduce_tr_entries(state, ref, acc, fun), to: __MODULE__.Protocol
end
