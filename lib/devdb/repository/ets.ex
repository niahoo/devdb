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

defimpl DevDB.Repository.Store, for: DevDB.Repository.Ets do
  import DevDB.Repository.Entry
  alias :ets, as: Ets

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
    match_bind =
      match_spec_base()
      |> put_elem(db_entry(:trref), ref)

    Ets.match_object(tab, match_bind)
    |> Enum.reduce(acc, fun)
  end
end
