defmodule DevDB.Repository.Ets.Entry do
  require Record
  Record.defrecord(:db_kv, key: nil, value: nil, trref: nil, trval: nil)
end

defmodule DevDB.Repository.Ets do
  alias :ets, as: Ets
  defstruct tab: nil
  import DevDB.Repository.Ets.Entry

  def new(_) do
    %__MODULE__{tab: :ets.new(__MODULE__, [:public, :set, {:keypos, db_kv(:key) + 1}])}
  end

  def put(%{tab: tab}, key, value) do
    true = Ets.insert(tab, [db_kv(key: key, value: value)])
    :ok
  end

  def delete(%{tab: tab}, key) do
    true = Ets.delete(tab, key)
    :ok
  end

  def fetch(%{tab: tab}, key) do
    case Ets.lookup(tab, key) do
      [db_kv(key: key, value: value)] ->
        {:ok, value}

      [] ->
        :error
    end
  end
end

defimpl DevDB.Repository, for: DevDB.Repository.Ets do
  import DevDB.Repository.Ets.Entry

  defdelegate put(repo, key, value), to: DevDB.Repository.Ets
  defdelegate delete(repo, key), to: DevDB.Repository.Ets
  defdelegate fetch(repo, key), to: DevDB.Repository.Ets
end
