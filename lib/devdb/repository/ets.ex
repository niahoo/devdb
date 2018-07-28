defmodule DevDB.Repository.Ets do
  import DevDB.Repository.Entry
  alias :ets, as: Ets

  def create_table(name, opts) do
    Ets.new(name, [:set, {:keypos, db_entry(:key) + 1} | opts])
  end

  defstruct tab: nil

  def new(tab) when is_reference(tab) do
    %__MODULE__{tab: tab}
  end
end

## --

## --

## --

defimpl DevDB.Repository.Store, for: DevDB.Repository.Ets do
  import DevDB.Repository.Entry
  alias :ets, as: Ets

  @match_tr_chunk_size 100
  def put_entry(%{tab: tab}, entry) do
    true = Ets.insert(tab, [entry])
    :ok
  end

  def delete_entry(%{tab: tab}, key) do
    true = Ets.delete(tab, key)
    :ok
  end

  def fetch_entry(%{tab: tab}, key) do
    case Ets.lookup(tab, key) do
      [rec | []] -> {:ok, rec}
      [] -> :error
    end
  end

  def reduce_entries(%{tab: tab}, acc, fun) do
    Ets.foldl(fun, acc, tab)
  end

  def reduce_tr_entries(%{tab: tab}, ref, acc, fun) do
    match_spec =
      match_spec_base()
      |> put_elem(db_entry(:trref), ref)

    Process.sleep(1000)
    # Unfolding expects one value to be givent at a time, but match_object
    # returns chunks of objects, so we use Stream concat
    Stream.unfold({:spec, match_spec}, fn
      {:spec, match_spec} ->
        unfold_match_result(Ets.match_object(tab, match_spec, @match_tr_chunk_size))

      {:cont, continuation} ->
        unfold_match_result(Ets.match_object(continuation))
    end)
    |> Stream.concat()
    |> Enum.reduce(acc, fun)
  end

  defp unfold_match_result({objects, continuation}) do
    {objects, {:cont, continuation}}
  end

  defp unfold_match_result(:"$end_of_table") do
    nil
  end
end
