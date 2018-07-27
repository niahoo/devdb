defmodule DevDB.Repository.Ets do
  require Logger
  alias :ets, as: Ets
  @todo "Add backend configuration, tab2file autodump, etc..."
  defstruct tab: nil,
            # current transaction reference
            ctrref: nil

  alias DevDB.Repository.Ets.Entry
  import Entry

  def create_table(name, opts) do
    Ets.new(name, [:set, {:keypos, db_entry(:key) + 1} | opts])
  end

  @m __MODULE__

  def new() do
    %@m{}
  end

  def accept_table(this, tab) when is_reference(tab) do
    %@m{this | tab: tab}
  end

  def put(%@m{tab: tab}, key, value) do
    true = Ets.insert(tab, [db_entry(key: key, value: value)])
    :ok
  end

  def delete(%@m{tab: tab}, key) do
    true = Ets.delete(tab, key)
    :ok
  end

  def fetch(%@m{tab: tab}, key) do
    case Ets.lookup(tab, key) do
      [db_entry(key: key, value: {_, :inserted_value})] -> :error
      [db_entry(key: key, value: value)] -> {:ok, value}
      [] -> :error
    end
  end

  # pre_filter_transform must transform an entry in {key, value} to be given to
  # the filter, or to :ignore to be filtered out.
  defp ets_filter_entries(tab, pre_filter_transform, filter) do
    reducer = fn entry, acc ->
      case pre_filter_transform.(entry) do
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
  def select(%@m{tab: tab}, filter) when is_function(filter, 2) do
    initial_selection = []

    pre_filter_transform = fn
      db_entry(key: key, value: value) ->
        {key, value}

      # catch all if we implement sequences or stuff that requires rows
      other ->
        Logger.warn("Selection against unknown entry format : #{inspect(other)}")
        :ignore
    end

    try do
      {:ok, ets_filter_entries(tab, pre_filter_transform, filter)}
    rescue
      e ->
        {:error, Exception.message(e)}
    catch
      :throw, e ->
        {:error, e}
    end
  end

  ## -- Transactional part ----------------------------------------------------

  # When entering a transaction, we simply change our repo data structure for
  # another module with different implementations of each functions.
  def begin_transaction(%@m{tab: tab, ctrref: nil} = this) do
    {:ok, %{this | ctrref: make_ref()}}
  end

  def tr_fetch(%@m{tab: tab, ctrref: ref}, key) do
    case Ets.lookup(tab, key) do
      [db_entry(key: key, trref: ^ref, trval: {^ref, :updated_value, new_value})] ->
        {:ok, new_value}

      [db_entry(key: key, value: value)] ->
        {:ok, value}

      [] ->
        :error
    end
  end

  def tr_select(%@m{tab: tab, ctrref: ref}, filter) when is_function(filter, 2) do
    initial_selection = []

    pre_filter_transform = fn
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
      {:ok, ets_filter_entries(tab, pre_filter_transform, filter)}
    rescue
      e ->
        {:error, Exception.message(e)}
    catch
      :throw, e ->
        {:error, e}
    end
  end

  def tr_delete(%@m{tab: tab, ctrref: ref} = this, key) do
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

  def tr_put(this = %@m{tab: tab, ctrref: ref}, key, value) do
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

  defp current_transient_objects(%@m{tab: tab, ctrref: ref} = this) do
    match_bind =
      match_spec_base()
      |> put_elem(db_entry(:trref), ref)

    # |> Enum.reduce(this, &commit_entry/2)
    Ets.match_object(tab, match_bind)
  end

  defp foreach_current_transient_object(%@m{tab: tab, ctrref: ref} = this, fun) do
    current_transient_objects(this)
    |> Enum.map_reduce(this, fn entry, acc ->
      {apply(fun, [entry, acc]), acc}
    end)
  end

  def commit_transaction(%@m{tab: tab, ctrref: ref} = this) when is_reference(ref) do
    foreach_current_transient_object(this, fn entry, thisp ->
      IO.puts("commit entry #{inspect(db_entry(entry, :key))}")
      commit_entry(entry, thisp)
    end)

    :ok
  end

  def rollback_transaction(%@m{tab: tab, ctrref: ref} = this) do
    foreach_current_transient_object(this, fn entry, thisp ->
      IO.puts("rollback entry #{inspect(db_entry(entry, :key))}")
      rollback_entry(entry, thisp)
    end)

    :ok
  end

  defp fetch_entry(tab, key) do
    case Ets.lookup(tab, key) do
      [rec] -> {:ok, rec}
      [] -> :no_record
    end
  end

  # Commit an update or an insert : we just migrate the value from the :trval
  # field to the :value field.
  defp commit_entry(
         db_entry(trval: {ref, :updated_value, value}) = entry,
         %@m{tab: tab, ctrref: ref} = acc
       ) do
    entry = db_entry(entry, value: value, trref: nil, trval: nil, trinserted: false)
    Ets.insert(tab, entry)
    acc
  end

  # Commit a deletion : we delete the record from the ETS table.
  defp commit_entry(
         db_entry(key: key, trval: {ref, :deleted_value}) = entry,
         %@m{tab: tab, ctrref: ref} = acc
       ) do
    Ets.delete(tab, key)
    acc
  end

  # Rolling back an inserted entry
  defp rollback_entry(
         db_entry(key: key, value: {ref, :inserted_value}),
         %@m{tab: tab, ctrref: ref} = acc
       ) do
    Ets.delete(tab, key)
    acc
  end

  # Rolling back a deleted or updated value, we just cleanup the record
  defp rollback_entry(entry, %@m{tab: tab, ctrref: ref} = acc) do
    entry = db_entry(entry, trref: nil, trval: nil, trinserted: false)
    Ets.insert(tab, entry)
    acc
  end
end
