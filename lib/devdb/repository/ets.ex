defmodule DevDB.Repository.Ets.Entry do
  require Record
  use TODO
  @todo "Remove ref entry as it is used in values ? But it's useful to match everything at once"
  Record.defrecord(:db_kv, key: nil, value: nil, trref: nil, trval: nil, trinserted: false)

  def match_spec_base() do
    db_kv(key: :_, value: :_, trref: :_, trval: :_, trinserted: :_)
  end

  # If in a transaction we delete a record, this value will be put in the :trval
  # field, showing that this record should be ignored
  def deleted_value(ref), do: {ref, :deleted_value}

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

  def new(opts) do
    tab =
      case Keyword.fetch(opts, :tab) do
        {:ok, ref} when is_reference(ref) -> ref
        _ -> Ets.new(__MODULE__, [:public, :set, {:keypos, db_kv(:key) + 1}])
      end

    %__MODULE__{tab: tab}
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

  # When entering a transaction, we simply change our repo data structure for
  # another module with different implementations of each functions.
  def begin_transaction(%{tab: tab}) do
    {:ok, DevDB.Repository.Ets.Transaction.new(tab: tab)}
  end
end

defmodule DevDB.Repository.Ets.Transaction do
  alias :ets, as: Ets
  defstruct tab: nil, ref: nil
  import DevDB.Repository.Ets.Entry

  def new(opts) do
    tab = Keyword.fetch!(opts, :tab)
    tr_reference = make_ref()
    %__MODULE__{tab: tab, ref: tr_reference}
  end

  def fetch(%{tab: tab, ref: ref}, key) do
    case Ets.lookup(tab, key) do
      [db_kv(key: key, trref: ^ref, trval: {^ref, :updated_value, new_value})] -> {:ok, new_value}
      [db_kv(key: key, value: value)] -> {:ok, value}
      [] -> :error
    end
  end

  def fetch_record(tab, key) do
    case Ets.lookup(tab, key) do
      [rec] -> {:ok, rec}
      [] -> :no_record
    end
  end

  def put(this = %{tab: tab, ref: ref}, key, value) do
    # We will put the data in the :trval field of the record, not in the actual
    # value. If the key doesn't exist, we must keep this information in order to
    # remove the record instead of just cleaning the transaction value.
    new_record =
      case fetch(this, key) do
        :error ->
          # Insert a new record
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

  def delete(%{tab: tab, ref: ref} = this, key) do
    case fetch_record(tab, key) do
      :no_record ->
        # nothing to delete
        :ok

      {:ok, db_kv(trval: {^ref, :deleted_value})} ->
        # already deleted
        :ok

      {:ok, entry} ->
        new_entry = db_kv(entry, trref: ref, trval: deleted_value(ref))
        true = Ets.insert(tab, [new_entry])
        :ok
    end
  end

  def commit_transaction(%{tab: tab, ref: ref} = this) do
    match_bind =
      match_spec_base()
      |> put_elem(db_kv(:trref), ref)
      |> IO.inspect()

    # |> Enum.reduce(this, &commit_entry/2)
    Ets.match_object(tab, match_bind)
    |> Enum.reduce(this, fn entry, acc ->
      IO.puts("Commit #{inspect(entry)}")
      commit_entry(entry, acc)
    end)

    new_nontransactional_repo = DevDB.Repository.Ets.new(tab: tab)
    {:ok, new_nontransactional_repo}
  end

  # Commit an update or an insert
  def commit_entry(
        db_kv(trval: {ref, :updated_value, value}) = entry,
        %{tab: tab, ref: ref} = acc
      ) do
    entry = db_kv(entry, value: value, trref: nil, trval: nil, trinserted: false)
    Ets.insert(tab, entry)
    acc
  end

  # Commit a deletion
  def commit_entry(
        db_kv(key: key, trval: {ref, :deleted_value}) = entry,
        %{tab: tab, ref: ref} = acc
      ) do
    Ets.delete(tab, key)
    acc
  end
end

defimpl DevDB.Repository, for: DevDB.Repository.Ets do
  defdelegate put(repo, key, value), to: DevDB.Repository.Ets
  defdelegate delete(repo, key), to: DevDB.Repository.Ets
  defdelegate fetch(repo, key), to: DevDB.Repository.Ets
  defdelegate begin_transaction(repo), to: DevDB.Repository.Ets

  def commit_transaction(_repo) do
    {:error, :unsupported}
  end
end

defimpl DevDB.Repository, for: DevDB.Repository.Ets.Transaction do
  defdelegate put(repo, key, value), to: DevDB.Repository.Ets.Transaction
  defdelegate delete(repo, key), to: DevDB.Repository.Ets.Transaction
  defdelegate fetch(repo, key), to: DevDB.Repository.Ets.Transaction
  defdelegate commit_transaction(repo), to: DevDB.Repository.Ets.Transaction

  def begin_transaction(_repo) do
    {:error, :unsupported}
  end
end
