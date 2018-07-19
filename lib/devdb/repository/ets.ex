defmodule DevDB.Repository.Ets.Entry do
  require Record
  use TODO
  @todo "Remove ref entry as it is used in values ? But it's useful to match everything at once"
  Record.defrecord(:db_kv, key: nil, value: nil, trref: nil, trval: nil, trinserted: false)

  # If in a transaction we delete a record, this value will be put in the :trval
  # field, showing that this record should be ignored
  defp deleted_value(ref), do: {ref, :deleted_value}

  # In case we operate a dirty read on a table where a transaction is in
  # progress, and in this transaction we inserted a value (in the :trval field),
  # the :value field of the record has not an actual value yet, so it will be
  # set to @ghost_value.
  def ghost_value(ref), do: {ref, :ghost_value}

  # Indicates that a new value has been inserted for this record in this
  # transaction
  def updated_value(value, ref), do: {ref, :updated_value, value}
end

defmodule DevDB.Repository.Ets do
  alias :ets, as: Ets
  defstruct tab: nil
  alias DevDB.Repository.Ets.Entry
  import Entry

  def new(_) do
    %__MODULE__{tab: Ets.new(__MODULE__, [:public, :set, {:keypos, db_kv(:key) + 1}])}
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
      [db_kv(key: key, value: {_, :ghost_value})] -> :error
      [db_kv(key: key, value: value)] -> {:ok, value}
      [] -> :error
    end
  end
end

defmodule DevDB.Repository.Ets.Transaction do
  alias :ets, as: Ets
  defstruct tab: nil, backup: nil, ref: nil
  import DevDB.Repository.Ets.Entry

  def new(tab) when is_reference(tab) do
    %__MODULE__{tab: tab, backup: create_backup_table(), ref: make_ref()}
  end

  defp create_backup_table() do
    nil
    # Ets.new(__MODULE__, [:set, :private])
  end

  def fetch(%{tab: tab, ref: ref}, key) do
    case Ets.lookup(tab, key) do
      [db_kv(key: key, trref: ^ref, trval: {^ref, :updated_value, new_value})] -> {:ok, new_value}
      [db_kv(key: key, value: value)] -> {:ok, value}
      [] -> :error
    end
  end

  def put(this = %{tab: tab, ref: ref}, key, value) do
    # We will put the data in the :trval field of the record, not in the actual
    # value. If the key doesn't exist, we must keep this information in order to
    # remove the record instead of just cleaning the transaction value.
    new_record =
      case fetch(this, key) do
        :error ->
          db_kv(
            key: key,
            value: ghost_value(ref),
            trinserted: true,
            trref: ref,
            trval: updated_value(value, ref)
          )

        {:ok, old_value} ->
          db_kv(
            key: key,
            value: old_value,
            trinserted: false,
            trref: ref,
            trval: updated_value(value, ref)
          )
      end

    true = Ets.insert(tab, [new_record])
    :ok
  end

  def delete(%{tab: tab}, key) do
    true = Ets.delete(tab, key)
    :ok
  end

  def get_commit_updates(%{tab: tab, ref: ref}) do
    [ref: ref]
  end
end

defimpl DevDB.Repository, for: DevDB.Repository.Ets do
  defdelegate put(repo, key, value), to: DevDB.Repository.Ets
  defdelegate delete(repo, key), to: DevDB.Repository.Ets
  defdelegate fetch(repo, key), to: DevDB.Repository.Ets

  def get_commit_updates(_repo) do
    {:error, :unsupported}
  end

  def cleanup_transaction(_repo) do
    {:error, :unsupported}
  end
end

defimpl DevDB.Repository, for: DevDB.Repository.Ets.Transaction do
  defdelegate put(repo, key, value), to: DevDB.Repository.Ets.Transaction
  defdelegate delete(repo, key), to: DevDB.Repository.Ets.Transaction
  defdelegate fetch(repo, key), to: DevDB.Repository.Ets.Transaction
  defdelegate get_commit_updates(repo), to: DevDB.Repository.Ets.Transaction
  defdelegate cleanup_transaction(repo), to: DevDB.Repository.Ets.Transaction
end
