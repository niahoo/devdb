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
  alias Kvern.Storage
  alias Kvern.Backup

  defmodule S do
    defstruct [:name, :config, :transaction_owner, :transaction_monitor, :backup, :storage]
  end

  @registry Kvern.Registry
  @confirm :ok

  @typedoc """
  Transaction reference
  """
  @type tref :: reference

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
      path: Keyword.get(config, :path, nil) |> validate_path!,
      name: Keyword.get(config, :name, nil),
      backup_conf: %{
        codec: Keyword.get(config, :codec, Kvern.Codec.Exs),
      }
    }
  end

  def validate_path!(path) do
    true = validate_path(path)
    path
  end

  def validate_path(nil), do: :ok
  def validate_path(path) do
    true = File.dir? path
  end

  def start_link(config) do
    config = validate_transform_config(config)
    start = fn() ->
      Process.flag(:trap_exit, true)
      name = Map.get(config, :name)
        |> IO.inspect
      case name do
        nil -> :ok
        name ->
          {:ok, _self} = Registry.register(@registry, name, __MODULE__)
      end
      init(config)
    end
    :plain_fsm.start_opt(__MODULE__, start, 1000, [:link])
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

  def init(config) do
    Logger.debug("#{__MODULE__} initializing ...")
    storage = Storage.new
    name = config.name
    state = ~M(%S config, storage, name)
    Logger.debug("#{__MODULE__} initialized.")
    {:reply, {:ok, self()}, fn ->
      state
        |> recover_storage()
        |> main_loop()
    end}
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
    # Logger.debug("Enter loop #{inspect state.name}")
    ~M(transaction_owner) = state # nil if not in transaction
    ereceive do
      # handling a command. The current transaction pid must be nil or be
      # equal to the caller's
      rcall(from, {:command, command})
          when transaction_owner in [nil, from_pid(from)] ->
        Logger.debug("Received command #{inspect command}")
        case handle_command(state, command) do
          {:continue, reply, new_state} ->
            reply_to(from, reply)
            new_state
            |> maybe_save_dirty(command)
            |> main_loop()
          {:rollback, reply} ->
            reply_to(from, reply)
            state
              |> transact_rollback() # if not in transaction, rollback does nothing
              |> ok!
              |> main_loop()
        end
      # Starting a transaction. Once the transaction is started, we send the
      # tref to the client
      rcall(from, {:begin, client_pid})
          when transaction_owner === nil ->
        client_pid = from_pid(from)
        {:ok, state} = transact_begin(state, client_pid)
        reply_to(from, @confirm)
        Logger.debug("Transaction started for #{inspect client_pid}")
        main_loop(state)
      rcall(from, :commit)
          when transaction_owner === from_pid(from) ->
        state = state |> transact_commit() |> ok!
        reply_to(from, @confirm)
        Logger.debug("Transaction committed for #{inspect from_pid(from)}")
        state
        |> save_to_disk()
        |> main_loop()
      rcall(from, :rollback)
          when transaction_owner === from_pid(from) ->
        {:ok, state} = transact_rollback(state)
        reply_to(from, @confirm)
        Logger.warn("Transaction rollback for #{inspect from_pid(from)}")
        main_loop(state)
      rcall(from, :shutdown)
          when transaction_owner === nil ->
        Logger.warn("Unregistering ...")
        Registry.unregister(@registry, state.name)
        Logger.warn("Shutting down ... #{inspect state.storage}")
        reply_to(from, :ok)
        :ok # ---------------------------- NO LOOP ---------------------
      rcall(from, :nuke_storage)
          when transaction_owner === nil ->
        Logger.warn("Nuking storage")
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
    # Logger.error("@todo implement")
    IO.puts("Kvern (#{inspect name}) storage:")
    IO.inspect storage.kvs
    IO.puts("tainted:")
    IO.inspect Storage.tainted(storage)
    IO.inspect state, pretty: true
  end

  defp transact_begin(state, client_pid) do
    # we will keep the current data in a safe place, and store the monitor ref
    # for the client to listen to DOWN messages.
    tref = make_ref()
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
  defp maybe_save_dirty(state, {:kv_put, _, _}), do: save_to_disk(state)
  # no need to write
  defp maybe_save_dirty(state, {:kv_get, _}), do: state
  defp maybe_save_dirty(state, {:kv_fetch, _}), do: state
  # don't know, so we should write
  defp maybe_save_dirty(state, command) do
    Logger.error("Unsure if command should write to disk : #{inspect command}, saving to disk")
    save_to_disk(state)
  end

  defp save_to_disk(%{config: %{path: nil}, name: name} = state) do
    Logger.debug("Disk backup configuration not found for store #{inspect name}")
    state
  end
  defp save_to_disk(%{config: %{path: false}, name: name} = state) do
    # backup disabled
    state
  end
  defp save_to_disk(%{config: %{path: path}, name: name, storage: storage} = state)
      when is_binary(path) do
      case Storage.tainted?(storage) do
        false -> state
        true ->
          saved_state = backup_to_disk(state, path)
          # try do
          #   saved_state
          # rescue
          #   e ->
          #   Logger.error(Exception.message(e))
          #   state
          # end
      end
  end

  defp backup_to_disk(state, path) do
    # Backup only the tainted elements.
    state.storage
      |> Storage.all_tainted_data()
      |> IO.inspect
      |> Stream.map(fn({key, value}) when is_binary(key) ->
          Backup.write_file(path, key, value, state.config.backup_conf)
         end)
      |> Stream.map(&log_backup_errors/1)
      |> Stream.run
    state
      |> Map.update!(:storage, &Storage.clear_tainted/1)
    state
  end

  defp log_backup_errors({:ok, key}) do
    Logger.debug("SAVED #{key}")
  end

  defp log_backup_errors({:error, {err, key}}) do
    Logger.error("SAVE ERROR #{key} : #{inspect err}")
  end


  defp recover_storage(%{config: %{path: nil}, name: name} = state) do
    Logger.debug("Disk backup configuration not found for store #{inspect name}")
    state
  end
  defp recover_storage(%{config: %{path: false}, name: name} = state) do
    # backup disabled
    state
  end
  defp recover_storage(%{config: %{path: path, backup_conf: bcf}, name: name, storage: storage} = state) do
    case Backup.recover_dir(path, bcf) do
      {:error, _} = err ->
        Logger.error("Could not recover storage for store #{inspect name} in #{path} : #{inspect err}")
        state
      {:ok, kvs} ->
        # Import all data but no need for the storage to be full tainted
        state = state
          |> Map.put(:storage, Storage.sys_import(kvs))
          |> Map.update!(:storage, &Storage.clear_tainted/1)
        Logger.warn("Recovered store from #{path} : #{inspect state.storage, pretty: true}")
        state
    end
  end

  defp nuke_storage(%{config: %{path: path}, name: name} = state)
      when is_binary(path) do
    with {:ok, _} <- File.rm_rf(path) do
      File.mkdir(path)
    else
      other -> {:error, other}
    end
  end
  defp nuke_storage(_), do: :ok # no storage

  # -- Internal Data Structures -----------------------------------------------

  defp run_command(state, {:kv_put, key, value}) do
    state
    |> Map.update!(:storage, &Storage.kv_put(&1, key, value))
    |> ok()
  end

  defp run_command(state, {:kv_fetch, key}) do
    {:reply, Storage.kv_fetch(state.storage, key)}
  end

  defp run_command(state, {:kv_get, key}) do
    {:reply, Storage.kv_get(state.storage, key)}
  end

  defp run_command(state, command) do
    raise "Unknown command #{inspect command}"
  end

end






