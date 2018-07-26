defmodule DevDB.Repository.Ets.Entry do
  require Record
  use TODO
  @todo "Remove ref entry as it is used in values ? But it's useful to match everything at once"
  Record.defrecord(:db_entry, key: nil, value: nil, trref: nil, trval: nil, trinserted: false)

  def match_spec_base() do
    db_entry(key: :_, value: :_, trref: :_, trval: :_, trinserted: :_)
  end

  # If in a transaction we delete a record, this value will be put in the :trval
  # field, showing that this record should be ignored
  def deleted_value(ref), do: {ref, :deleted_value}

  # In case we operate a dirty read on a table where a transaction is in
  # progress, and in this transaction we inserted a value (in the :trval field),
  # the :value field of the record has not an actual value yet, so it will be
  # set to @inserted_value.
  def inserted_value(ref), do: {ref, :inserted_value}

  # Indicates that a new value has been inserted for this record in this
  # transaction
  def updated_value(value, ref), do: {ref, :updated_value, value}
end

defmodule DevDB.Repository.Ets do
  require Logger
  alias :ets, as: Ets
  defstruct tab: nil
  alias DevDB.Repository.Ets.Entry
  import Entry

  def new(opts) do
    tab =
      case Keyword.fetch(opts, :tab) do
        {:ok, ref} when is_reference(ref) -> ref
        _ -> Ets.new(__MODULE__, [:public, :set, {:keypos, db_entry(:key) + 1}])
      end

    %__MODULE__{tab: tab}
  end

  def put(%{tab: tab}, key, value) do
    true = Ets.insert(tab, [db_entry(key: key, value: value)])
    :ok
  end

  def delete(%{tab: tab}, key) do
    true = Ets.delete(tab, key)
    :ok
  end

  def fetch(%{tab: tab}, key) do
    case Ets.lookup(tab, key) do
      [db_entry(key: key, value: {_, :inserted_value})] -> :error
      [db_entry(key: key, value: value)] -> {:ok, value}
      [] -> :error
    end
  end

  def ets_filter_entries(tab, get_selectables, filter) do
    reducer = fn entry, acc ->
      case get_selectables.(entry) do
        :ignore ->
          acc

        {key, value} ->
          if filter.(value, key) do
            [{key, value} | acc]
          else
            acc
          end
      end
    end

    Ets.foldl(reducer, [], tab)
  end

  # For now, the select system is very naive and so very slow, the user provides
  # a fn/2 accepting value and key (in this order), and returns true or false to
  # include or not the record in the selection. We do a full table scan and call
  # the fun on every value in the table.
  def select(%{tab: tab}, filter) when is_function(filter, 2) do
    initial_selection = []

    get_selectables = fn
      db_entry(key: key, value: value) ->
        {key, value}

      # catch all if we implement sequences or stuff that requires rows
      other ->
        Logger.warn("Selection against unknown entry format : #{inspect(other)}")
        :ignore
    end

    try do
      {:ok, ets_filter_entries(tab, get_selectables, filter)}
    rescue
      e ->
        {:error, Exception.message(e)}
    catch
      :throw, e ->
        {:error, e}
    end
  end

  # When entering a transaction, we simply change our repo data structure for
  # another module with different implementations of each functions.
  def begin_transaction(%{tab: tab}) do
    {:ok, DevDB.Repository.Ets.Transaction.new(tab: tab)}
  end
end

defmodule DevDB.Repository.Ets.Transaction do
  require Logger
  alias :ets, as: Ets
  defstruct tab: nil, ref: nil
  import DevDB.Repository.Ets.Entry
  alias DevDB.Repository.Ets, as: Parent

  def new(opts) do
    tab = Keyword.fetch!(opts, :tab)
    tr_reference = make_ref()
    %__MODULE__{tab: tab, ref: tr_reference}
  end

  def fetch(%{tab: tab, ref: ref}, key) do
    case Ets.lookup(tab, key) do
      [db_entry(key: key, trref: ^ref, trval: {^ref, :updated_value, new_value})] ->
        {:ok, new_value}

      [db_entry(key: key, value: value)] ->
        {:ok, value}

      [] ->
        :error
    end
  end

  def fetch_entry(tab, key) do
    case Ets.lookup(tab, key) do
      [rec] -> {:ok, rec}
      [] -> :no_record
    end
  end

  def select(%{tab: tab, ref: ref}, filter) when is_function(filter, 2) do
    initial_selection = []

    get_selectables = fn
      db_entry(key: key, trval: {^ref, :deleted_value}) ->
        :ignore

      db_entry(key: key, trval: {^ref, :updated_value, value}) ->
        {key, value}

      db_entry(key: key, value: value, trval: nil) ->
        {key, value}

      # catch all if we implement sequences or stuff that requires rows
      other ->
        Logger.warn("Selection against unknown entry format : #{inspect(other)}")
        :ignore
    end

    try do
      {:ok, Parent.ets_filter_entries(tab, get_selectables, filter)}
    rescue
      e ->
        {:error, Exception.message(e)}
    catch
      :throw, e ->
        {:error, e}
    end
  end

  def put(this = %{tab: tab, ref: ref}, key, value) do
    # We will put the data in the :trval field of the record, not in the actual
    # value. If the key doesn't exist, we must keep this information in order to
    # remove the record instead of just cleaning the transaction value.
    new_record =
      case fetch_entry(tab, key) do
        :no_record ->
          # Insert a new record
          db_entry(
            key: key,
            value: inserted_value(ref),
            trinserted: true,
            trref: ref,
            trval: updated_value(value, ref)
          )

        {:ok, entry} ->
          db_entry(
            entry,
            trref: ref,
            trval: updated_value(value, ref)
          )
      end

    true = Ets.insert(tab, [new_record])
    :ok
  end

  def delete(%{tab: tab, ref: ref} = this, key) do
    case fetch_entry(tab, key) do
      :no_record ->
        # nothing to delete
        :ok

      {:ok, db_entry(trval: {^ref, :deleted_value})} ->
        # already deleted
        :ok

      {:ok, entry} ->
        new_entry = db_entry(entry, trref: ref, trval: deleted_value(ref))
        true = Ets.insert(tab, [new_entry])
        :ok
    end
  end

  defp current_transient_objects(%{tab: tab, ref: ref} = this) do
    match_bind =
      match_spec_base()
      |> put_elem(db_entry(:trref), ref)

    # |> Enum.reduce(this, &commit_entry/2)
    Ets.match_object(tab, match_bind)
  end

  defp foreach_current_transient_object(%{tab: tab, ref: ref} = this, {mod, fun, args}) do
    current_transient_objects(this)
    |> Enum.map_reduce(this, fn entry, acc ->
      IO.puts("#{fun} #{inspect(db_entry(entry, :key))}")
      {apply(mod, fun, [entry, acc | args]), acc}
    end)
  end

  def commit_transaction(%{tab: tab, ref: ref} = this) do
    foreach_current_transient_object(this, {__MODULE__, :commit_entry, []})
    new_nontransactional_repo = Parent.new(tab: tab)
    {:ok, new_nontransactional_repo}
  end

  # Commit an update or an insert : we just migrate the value from the :trval
  # field to the :value field.
  def commit_entry(
        db_entry(trval: {ref, :updated_value, value}) = entry,
        %{tab: tab, ref: ref} = acc
      ) do
    entry = db_entry(entry, value: value, trref: nil, trval: nil, trinserted: false)
    Ets.insert(tab, entry)
    acc
  end

  # Commit a deletion : we delete the record from the ETS table.
  def commit_entry(
        db_entry(key: key, trval: {ref, :deleted_value}) = entry,
        %{tab: tab, ref: ref} = acc
      ) do
    Ets.delete(tab, key)
    acc
  end

  def rollback_transaction(%{tab: tab, ref: ref} = this) do
    foreach_current_transient_object(this, {__MODULE__, :rollback_entry, []})
    new_nontransactional_repo = Parent.new(tab: tab)
    {:ok, new_nontransactional_repo}
  end

  # Rolling back an inserted entry
  def rollback_entry(
        db_entry(key: key, value: {ref, :inserted_value}),
        %{tab: tab, ref: ref} = acc
      ) do
    Ets.delete(tab, key)
    acc
  end

  # Rolling back a deleted or updated value, we just cleanup the record
  def rollback_entry(entry, %{tab: tab, ref: ref} = acc) do
    entry = db_entry(entry, trref: nil, trval: nil, trinserted: false)
    Ets.insert(tab, entry)
    acc
  end
end

defimpl DevDB.Repository, for: DevDB.Repository.Ets do
  defdelegate put(repo, key, value), to: DevDB.Repository.Ets
  defdelegate delete(repo, key), to: DevDB.Repository.Ets
  defdelegate fetch(repo, key), to: DevDB.Repository.Ets
  defdelegate select(repo, filter), to: DevDB.Repository.Ets
  defdelegate begin_transaction(repo), to: DevDB.Repository.Ets

  def commit_transaction(_repo) do
    {:error, :unsupported}
  end

  def rollback_transaction(_repo) do
    {:error, :unsupported}
  end
end

defimpl DevDB.Repository, for: DevDB.Repository.Ets.Transaction do
  defdelegate put(repo, key, value), to: DevDB.Repository.Ets.Transaction
  defdelegate delete(repo, key), to: DevDB.Repository.Ets.Transaction
  defdelegate fetch(repo, key), to: DevDB.Repository.Ets.Transaction
  defdelegate select(repo, filter), to: DevDB.Repository.Ets.Transaction
  defdelegate commit_transaction(repo), to: DevDB.Repository.Ets.Transaction
  defdelegate rollback_transaction(repo), to: DevDB.Repository.Ets.Transaction

  def begin_transaction(_repo) do
    {:error, :unsupported}
  end
end
