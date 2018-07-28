defprotocol DevDB.Repository.Store do
  def put_entry(state, entry)
  def delete_entry(state, key)
  def fetch_entry(state, key)
  def reduce_entries(state, acc, fun)
  def reduce_tr_entries(state, ref, acc, fun)
end

defmodule DevDB.Repository do
  require Logger
  alias DevDB.Repository.Store
  @todo "Add backend configuration, tab2file autodump, etc..."
  defstruct main: nil,
            # current transaction reference
            ctrref: nil

  alias DevDB.Repository.Entry
  import Entry

  @m __MODULE__

  def new() do
    %@m{}
  end

  def set_main_store(this, main) do
    %@m{this | main: main}
  end

  def put(%@m{main: main}, key, value) do
    :ok = Store.put_entry(main, db_entry(key: key, value: value))
  end

  def delete(%@m{main: main}, key) do
    :ok = Store.delete_entry(main, key)
  end

  def fetch(%@m{main: main}, key) do
    case Store.fetch_entry(main, key) do
      {:ok, db_entry(key: key, value: {_, :inserted_value})} -> :error
      {:ok, db_entry(key: key, value: value)} -> {:ok, value}
      :error -> :error
    end
  end

  # pre_filter_transform must transform an entry in {key, value} to be given to
  # the filter, or to :ignore to be filtered out.
  defp filter_entries(main, pre_filter_transform, filter) do
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

    Store.reduce_entries(main, [], reducer)
  end

  # For now, the select system is very naive and so very slow, the user provides
  # a fn/2 accepting value and key (in this order), and returns true or false to
  # include or not the record in the selection. We do a full table scan and call
  # the fun on every value in the table.
  def select(%@m{main: main}, filter) when is_function(filter, 2) do
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
      {:ok, filter_entries(main, pre_filter_transform, filter)}
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
  def begin_transaction(%@m{ctrref: nil} = this) do
    {:ok, %{this | ctrref: make_ref()}}
  end

  def tr_fetch(%@m{main: main, ctrref: ref}, key) do
    case Store.fetch_entry(main, key) do
      {:ok, db_entry(key: key, trref: ^ref, trval: {^ref, :updated_value, new_value})} ->
        {:ok, new_value}

      {:ok, db_entry(key: key, value: value)} ->
        {:ok, value}

      :error ->
        :error
    end
  end

  def tr_select(%@m{main: main, ctrref: ref}, filter) when is_function(filter, 2) do
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
      {:ok, filter_entries(main, pre_filter_transform, filter)}
    rescue
      e ->
        {:error, Exception.message(e)}
    catch
      :throw, e ->
        {:error, e}
    end
  end

  def tr_delete(%@m{main: main, ctrref: ref} = this, key) do
    case Store.fetch_entry(main, key) do
      :error ->
        # nothing to delete
        :ok

      {:ok, db_entry(trval: {^ref, :deleted_value})} ->
        # already deleted
        :ok

      {:ok, entry} ->
        new_entry = db_entry(entry, trref: ref, trval: deleted_value(ref))
        :ok = Store.put_entry(main, new_entry)
        :ok
    end
  end

  def tr_put(this = %@m{main: main, ctrref: ref}, key, value) do
    # We will put the data in the :trval field of the record, not in the actual
    # value. If the key doesn't exist, we must keep this information in order to
    # remove the record instead of just cleaning the transaction value.
    new_record =
      case Store.fetch_entry(main, key) do
        :error ->
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

    :ok = Store.put_entry(main, new_record)
  end

  defp foreach_current_transient_object(%@m{main: main, ctrref: ref} = this, fun) do
    Store.reduce_tr_entries(main, ref, this, fn entry, this ->
      apply(fun, [entry, this])
      this
    end)
  end

  def commit_transaction(%@m{ctrref: ref} = this) when is_reference(ref) do
    foreach_current_transient_object(this, fn entry, thisp ->
      IO.puts("commit entry #{inspect(db_entry(entry, :key))}")
      commit_entry(entry, thisp)
    end)

    :ok
  end

  def rollback_transaction(%@m{ctrref: ref} = this) when is_reference(ref) do
    foreach_current_transient_object(this, fn entry, thisp ->
      IO.puts("rollback entry #{inspect(db_entry(entry, :key))}")
      rollback_entry(entry, thisp)
    end)

    :ok
  end

  # Commit an update or an insert : we just migrate the value from the :trval
  # field to the :value field.
  defp commit_entry(
         db_entry(trval: {ref, :updated_value, value}) = entry,
         %@m{main: main, ctrref: ref} = acc
       ) do
    entry = db_entry(entry, value: value, trref: nil, trval: nil, trinserted: false)
    :ok = Store.put_entry(main, entry)
  end

  # Commit a deletion : we delete the record from the ETS table.
  defp commit_entry(
         db_entry(key: key, trval: {ref, :deleted_value}) = entry,
         %@m{main: main, ctrref: ref} = acc
       ) do
    :ok = Store.delete_entry(main, key)
  end

  # Rolling back an inserted entry
  defp rollback_entry(
         db_entry(key: key, value: {ref, :inserted_value}),
         %@m{main: main, ctrref: ref} = acc
       ) do
    :ok = Store.delete_entry(main, key)
  end

  # Rolling back a deleted or updated value, we just cleanup the record
  defp rollback_entry(entry, %@m{main: main, ctrref: ref} = acc) do
    entry = db_entry(entry, trref: nil, trval: nil, trinserted: false)
    :ok = Store.put_entry(main, entry)
  end
end
