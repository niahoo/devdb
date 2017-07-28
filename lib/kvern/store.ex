defmodule GenHelp do
  defmacro rcall(from, msg) do
    quote do
      {:'$gen_call', unquote(from), unquote(msg)}
    end
  end
  defmacro rcast(msg) do
    quote do
      {:'$gen_cast', unquote(msg)}
    end
  end
  def reply_to(from, rep) do
    GenServer.reply(from, rep)
  end
  defmacro from_pid(from) do
    # from is {pid, ref}
    quote do
      elem(unquote(from), 0)
    end
  end
end

defmodule Kvern.Store do
  use PlainFsm
  require Logger
  import GenHelp
  import ShorterMaps
  alias Kvern.Backup

  defmodule S do
    defstruct [
      name: nil,
      config: nil,
      transaction_owner: nil,
      transaction_monitor: nil,
      backup: nil,
      storage: %{},
      tainted: [],
      deleted: [],
    ]
  end

  @registry Kvern.Registry
  @confirm :ok

  # -- Client side API --------------------------------------------------------

  def via(pid) when is_pid(pid),
    do: pid
  def via(name),
    do: {:via, Registry, {@registry, name}}

  defp ok!({:ok, val}), do: val

  defp ok({:error, _} = err), do: err
  defp ok(:ok), do: :ok
  defp ok({:ok, val}), do: {:ok, val}
  defp ok(val), do: {:ok, val}

  def validate_transform_config(config) do
    %{
      user_handlers: Keyword.get(config, :handlers, %{}),
      dir: Keyword.get(config, :dir, nil) |> validate_dir!,
      name: Keyword.get(config, :name, nil),
      backup_conf: %{
        codec: Keyword.get(config, :codec, Kvern.Codec.Exs),
        codec_encode_opts: Keyword.get(config, :codec_encode_opts, []),
        codec_decode_opts: Keyword.get(config, :codec_decode_opts, []),
      }
    }
  end

  def validate_dir!(dir) do
    if validate_dir(dir) do
      dir
    else
      raise "Bad directory for kvern: #{inspect dir}"
    end
  end

  def validate_dir(nil), do: true
  def validate_dir(false), do: true
  def validate_dir(dir) do
    File.dir? dir
  end

  def start_link(args) do
    start = fn() ->
      init(args)
    end
    res = :plain_fsm.start_opt(__MODULE__, start, 1000, [:link])
    res
  end

  def send_command(db, command) do
    GenServer.call(via(db), {:command, command})
  end

  def begin(db) do
    case GenServer.call(via(db), {:begin, self()}) do
      @confirm -> :ok
      other -> other
    end
  end

  def commit(db) do
    case GenServer.call(via(db), :commit) do
      @confirm -> :ok
      other -> other
    end
  end

  def rollback(db) do
    case GenServer.call(via(db), :rollback) do
      @confirm -> :ok
      other -> other
    end
  end

  # -- Database initialization ------------------------------------------------

  def init(args) do
    config = validate_transform_config(args)
    name = Map.get(config, :name)
    case name do
      nil -> :ok
      name ->
        {:ok, _self} = Registry.register(@registry, name, __MODULE__)
    end
    inited = ~M(%S config, name)
    |> recover_storage()
    |> case do
        {:ok, state} ->
          {:reply, {:ok, self()}, fn -> main_loop(state) end}
        {:error, _} = err ->
          {:reply, err, fn -> exit(:could_not_recover_storage) end}
       end
  end

  def data_vsn(),
    do: 5

  def code_change(_OldVsn, _State, _Extra) do
    {:ok, {:newstate, _State}}
  end

  # -- Server States ----------------------------------------------------------

  def main_loop(nil) do
    raise "bad state"
  end

  def main_loop(state) do
    ~M(transaction_owner) = state # nil if not in transaction
    ereceive do
      # handling a command. The current transaction pid must be nil or be
      # equal to the caller's
      rcall(from, {:command, command})
          when transaction_owner in [nil, from_pid(from)] ->
        case handle_command(state, command) do
          {:continue, reply, new_state} ->
            state = new_state
              |> maybe_save_dirty(command)
            reply_to(from, reply)
            main_loop(state)
          {:rollback, reply} ->
            reply_to(from, reply)
            state
              |> transact_rollback() # if not in transaction, rollback does nothing
              |> ok!
              |> main_loop()
        end
      # Starting a transaction.
      rcall(from, {:begin, client_pid})
          when transaction_owner === nil ->
        {:ok, state} = transact_begin(state, client_pid)
        reply_to(from, @confirm)
        main_loop(state)
      rcall(from, :commit)
          when transaction_owner === from_pid(from) ->
        state = state
          |> transact_commit()
          |> ok!
          |> save_to_disk()
        reply_to(from, @confirm)
        main_loop(state)
      rcall(from, :rollback)
          when transaction_owner === from_pid(from) ->
        {:ok, state} = transact_rollback(state)
        reply_to(from, @confirm)
        Logger.warn("Transaction rollback for #{inspect from_pid(from)}")
        main_loop(state)
      rcall(from, :shutdown)
          when transaction_owner === nil ->
        Registry.unregister(@registry, state.name)
        reply_to(from, :ok)
        :ok # ---------------------------- NO LOOP ---------------------
      rcall(from, :nuke_storage)
          when transaction_owner === nil ->
        reply_to(from, nuke_storage(state))
        main_loop(state)
      rcast(:print_dump) ->
        print_dump(state)
        main_loop(state)
      rcall(from, other)
          when transaction_owner in [nil, from_pid(from)] ->
        Logger.warn("Database received unexpected call #{inspect other} from #{inspect from_pid(from)}")
        reply_to(from, {:error, {:unhandled_message, other}})
        main_loop(state)
      rcast(other) ->
        Logger.warn("Database received unexpected cast #{inspect other}")
        main_loop(state)

      # Here is no catchall call because we do not want to match on any
      # message when a transaction is running. This way, those messages will
      # wait in the mailbox that the transaction is over
    end
  end

  defp print_dump(state) do
    ~M(name, storage) = state
    IO.puts "Dump store #{name}"
    codec = state.config.backup_conf.codec
    codec_encode_opts = state.config.backup_conf.codec_encode_opts
    storage
      |> Enum.map(fn {k, v} ->
          [
            "[", k, "]\n",
            case codec.encode(v, codec_encode_opts) do
              {:ok, str} -> str
              {:error, _} = err -> inspect(err)
            end,
            "\n"
          ]
         end)
      |> IO.puts
  end

  defp transact_begin(state, client_pid) do
    # we will keep the current data in a safe place, and store the monitor ref
    # for the client to listen to DOWN messages.
    mref = Process.monitor(client_pid)
    ~M(storage) = state
    state = state
      |> Map.put(:transaction_owner, client_pid)
      |> Map.put(:transaction_monitor, mref)
      |> Map.put(:backup, storage)
    {:ok, state}
  end

  defp transact_rollback(%{transaction_owner: nil} = state) do
    # Not in transaction, cannot rollback
    {:ok, state}
  end
  defp transact_rollback(state) do
    # set the old storage back in storage
    backup = state.backup
    transact_cleanup(state, backup)
  end

  defp transact_commit(state) do
    # confirm that the current storage is the good one
    ~M(storage) = state
    transact_cleanup(state, storage)
  end

  defp transact_cleanup(state, storage) do
    # put back the untainted storage in the state, forget all
    # modifications since the begining of the transaction
    Process.demonitor(state.transaction_monitor, [:flush])
    state = %S{
      state |
      transaction_owner: nil,
      transaction_monitor: nil,
      backup: nil,
      storage: storage,
    }
    {:ok, state}
  end


  defp handle_command(state, command) do
    try do
      run_command(state, command)
    rescue
        e in _ ->
          msg = Exception.message(e)
          Logger.error(msg)
        reply = {:error, {:command_exception, command, e}}
        {:rollback, reply}
    else
      {:ok, reply, new_state} ->
        {:continue, reply, new_state}
      {:ok, new_state} ->
        {:continue, @confirm, new_state}
      {:reply, reply} ->
        {:continue, reply, state}
      {:error, _} = err ->
        {:rollback, err}
      other ->
        reply = {:error, {:unknown_result, other}}
        {:rollback, reply}
    end
  end

  # -- Writing to file --------------------------------------------------------

  # After dirty writes, we must save instead of wait for a commit.
  # if we are in a transaction, we skip
  # if the command is a read operation, we skip

  defp maybe_save_dirty(%{transaction_owner: pid} = state, _) when is_pid(pid) do
    # we are in a transaction
    state
  end
  # must write
  defp maybe_save_dirty(state, {:kv_write, _, _}), do: save_to_disk(state)
  defp maybe_save_dirty(state, {:kv_put, _, _}), do: save_to_disk(state)
  defp maybe_save_dirty(state, {:kv_delete, _}), do: save_to_disk(state)
  # no need to write
  defp maybe_save_dirty(state, {:kv_read, _, _}), do: state
  # don't know, so we should write
  defp maybe_save_dirty(state, command) do
    Logger.warn("Unsure if command should write to disk : #{inspect command}, saving to disk")
    save_to_disk(state)
  end

  defp save_to_disk(%{config: %{dir: nil}, name: name} = state) do
    Logger.warn("Disk backup configuration not found for store #{inspect name}")
    state
  end
  defp save_to_disk(%{config: %{dir: false}, name: name} = state) do
    # backup disabled
    state
  end
  defp save_to_disk(%{config: %{dir: dir}, name: name, storage: storage} = state)
      when is_binary(dir) do
      case storage_tainted?(state) do
        false -> state
        true ->
          try do
            saved_state = backup_to_disk(state, dir)
            saved_state
          rescue
            e ->
            Logger.error(Exception.message(e))
            state
          end
      end
  end

  defp backup_to_disk(state, dir) do
    # Backup only the tainted elements.
    ~M(tainted, deleted, storage, config) = state
    ~M(backup_conf) = config

    tainted
      |> Stream.map(fn(key) when is_binary(key) ->
          value = Map.fetch!(storage, key)
          Backup.write_file(dir, key, value, backup_conf)
         end)
      |> Enum.map(&log_backup_errors/1)

    deleted
      |> Stream.map(fn(key) when is_binary(key) ->
          Backup.delete_file(dir, key, backup_conf)
         end)
      |> Enum.map(&log_backup_errors/1)

    state
      |> Map.put(:tainted, [])
      |> Map.put(:deleted, [])
  end

  defp log_backup_errors({:ok, key}) do
  end

  defp log_backup_errors({:error, {err, key}}) do
    Logger.error("SAVE ERROR #{key} : #{inspect err}")
  end


  defp recover_storage(%{config: %{dir: nil}, name: name} = state) do
    Logger.warn("Disk backup configuration not found for store #{inspect name}")
    {:ok, state}
  end
  defp recover_storage(%{config: %{dir: false}, name: name} = state) do
    # backup disabled
    {:ok, state}
  end
  defp recover_storage(%{config: %{dir: dir, backup_conf: bcf}, name: name, storage: storage} = state) do
    case Backup.recover_dir(dir, bcf) do
      {:error, _} = err ->
        Logger.error("Could not recover storage for store #{inspect name} in #{dir} : #{inspect err}")
        err
      {:ok, kvs} ->
        # Import all data but no need for the storage to be full tainted
        state = state
          |> Map.put(:storage, kvs)
        keys_list = kvs
          |> Enum.map(fn {k,_} -> "* " <> k end)
          |> Enum.join("\n")
        {:ok, state}
    end
  end

  defp nuke_storage(%{config: %{dir: dir}, name: name} = state)
      when is_binary(dir) do
    with {:ok, _} <- File.rm_rf(dir) do
      File.mkdir(dir)
    else
      other -> {:error, other}
    end
  end
  defp nuke_storage(_), do: :ok # no storage

  defp run_command(state, {:kv_put, key, value}) do
    state
    |> Map.update!(:storage, &Map.put(&1, key, value))
    |> mark_tainted(key)
    |> ok()
  end

  defp run_command(state, {:kv_delete, key}) do
    state
    |> Map.update!(:storage, &Map.delete(&1, key))
    |> mark_deleted(key)
    |> ok()
  end

  defp run_command(state, {:kv_read, afun, args}) do
    {:reply, apply(Map, afun, [state.storage|args])}
  end

  defp run_command(state, command) do
    raise "Unknown command #{inspect command}"
  end

  defp mark_tainted(state, key) do
    Map.update!(state, :tainted, fn list -> [key|list] end)
  end

  defp mark_deleted(state, key) do
    Map.update!(state, :deleted, fn list -> [key|list] end)
  end

  defp storage_tainted?(%{tainted: [], deleted: []}), do: false
  defp storage_tainted?(_), do: true

end






