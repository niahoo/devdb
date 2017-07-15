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
      path: Keyword.get(config, :path, nil) |> validate_path,
      name: Keyword.get(config, :name, nil),
    }
  end

  def validate_path(nil), do: :ok
  def validate_path(path) do
    true = File.dir? path
  end

  def start_link(config) do
    config = validate_transform_config(config)
    start = fn() ->
      Process.flag(:trap_exit, true)
      case Map.get(config, :name) do
        nil -> :ok
        name -> Registry.register(@registry, name, __MODULE__)
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
    state = ~M(%S config, storage)
    Logger.debug("#{__MODULE__} initialized.")
    {:reply, {:ok, self()}, fn -> main_loop(state) end}
  end

  def data_vsn(),
    do: 5

  def code_change(_OldVsn, _State, _Extra) do
    {:ok, {:newstate, _State}}
  end

  # -- Server States ----------------------------------------------------------

  def main_loop(state) do
    # Logger.debug("Enter loop #{inspect state}")
    ~M(transaction_owner) = state # nil if not in transaction
    ereceive do
      # handling a command. The current transaction pid must be nil or be
      # equal to the caller's
      rcall(from, {:command, command})
          when transaction_owner in [nil, from_pid(from)] ->
        case handle_command(state, command) do
          {:continue, reply, new_state} ->
            reply_to(from, reply)
            main_loop(new_state)
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
        # |> maybe_save_to_file()
        |> main_loop()
      rcall(from, :rollback)
          when transaction_owner === from_pid(from) ->
        {:ok, state} = transact_rollback(state)
        reply_to(from, @confirm)
        Logger.warn("Transaction rollback for #{inspect from_pid(from)}")
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
    after 10000 ->
      Logger.debug("timeout in main_loop")
      main_loop(state)
    end
  end

  defp print_dump(state) do
    ~M(name, storage) = state
    # Logger.error("@todo implement")
    IO.puts("Kvern (#{inspect name}) storage:")
    IO.inspect storage.kvs
    IO.puts("tainted:")
    IO.inspect Storage.tainted(storage)
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

  defp maybe_save_to_file(%{config: %{path: nil}, name: name} = state) do
    Logger.debug("Disk backup configuration not found for database #{inspect name}")
    state
  end
  defp maybe_save_to_file(%{config: %{path: false}, name: name} = state) do
    state
  end
  defp maybe_save_to_file(%{config: %{path: path}, name: name} = state)
      when is_binary(path) do
      _saved = save_to_file(state, path)
    try do
      _saved
    rescue
      e ->
      Logger.error(Exception.message(e))
      state
    end
  end

  defp save_to_file(state, path) do
    # ~M(schema, storage) = state
    # Backup.write(~M(schema, storage, path))
    # state
  end

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






