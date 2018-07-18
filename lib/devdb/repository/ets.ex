defmodule DevDB.Repository.Ets.Entry do
  require Record
  Record.defrecord :db_kv, key: nil, value: nil, trref: nil, trval: nil
end

defmodule DevDB.Repository.Ets do
  defstruct tab: nil
  import DevDB.Repository.Ets.Entry


  def new(_) do
    %__MODULE__{tab: :ets.new(__MODULE__, [:public, :set, {:keypos, db_kv(:key)}])}
  end
end

defimpl DevDB.Repository, for: DevDB.Repository.Ets do
  import DevDB.Repository.Ets.Entry
  def put(%{tab: tab}, key, value) do
    true = :ets.insert(tab, [db_kv(key: key, value: value)])
    :ok
  end
end
