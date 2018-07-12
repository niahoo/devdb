defmodule Kvern.Store do
  require Logger
  import ShorterMaps
  use GenLoop, enter: :main_loop
  alias Kvern.Backup

  defmodule S do
    defstruct name: nil,
              config: nil,
              transaction_owner: nil,
              transaction_monitor: nil,
              backup: nil,
              storage: %{},
              tainted: [],
              deleted: []
  end

  @registry Kvern.Registry
  @confirm :ok

  # -- Client side API --------------------------------------------------------

  def via(pid) when is_pid(pid), do: pid
  def via(name), do: {:via, Registry, {@registry, name}}
  def whereis(name), do: Registry.whereis_name({@registry, name})

  defp uwok!({:ok, val}), do: val

  defp wok({:error, _} = err), do: err
  defp wok(:ok), do: :ok
  defp wok({:ok, val}), do: {:ok, val}
  defp wok(val), do: {:ok, val}

  def start_link(opts) do
    gen_opts =
      Keyword.take(opts, [:name])
      |> Keyword.update(:name, nil, fn name -> via(name) end)

    IO.puts("gen_opts #{inspect(gen_opts)}")
    GenLoop.start_link(__MODULE__, opts, gen_opts)
  end

  def send_command(db, command) do
    GenLoop.call(via(db), {:command, command})
  end

  def begin(db) do
    case GenLoop.call(via(db), {:begin, self()}) do
      @confirm -> :ok
      other -> other
    end
  end

  def commit(db) do
    case GenLoop.call(via(db), :commit) do
      @confirm -> :ok
      other -> other
    end
  end

  def rollback(db) do
    case GenLoop.call(via(db), :rollback) do
      @confirm -> :ok
      other -> other
    end
  end

  def get_state(db) do
    GenLoop.call(via(db), :get_state)
  end

  # -- Database initialization ------------------------------------------------

  def init(opts) do
    name = Keyword.get(opts, :name)
    IO.puts("Initializing store #{inspect(name)}")

    state = %S{config: opts, name: name}
    {:ok, state}
  end

  def data_vsn(), do: 5

  def code_change(_oldvsn, state, _extra) do
    {:ok, {:newstate, state}}
  end

  # -- Server States ----------------------------------------------------------

  def main_loop(nil) do
    raise "bad state"
  end

  def main_loop(state) do
    # nil if not in transaction
    ~M(transaction_owner) = state

    receive state do
      # handling a command. The current transaction pid must be nil or be
      # equal to the caller's
      rcall(from, {:command, command})
      when transaction_owner in [nil, from_pid(from)] ->
        case handle_command(state, command) do
          {:continue, reply, new_state} ->
            state =
              new_state
              |> maybe_save_dirty(command)

            reply(from, reply)
            main_loop(state)

          {:rollback, reply} ->
            reply(from, reply)
            # if not in transaction, rollback does nothing
            state
            |> transact_rollback()
            |> uwok!
            |> main_loop()
        end

      # Starting a transaction.
      rcall(from, {:begin, client_pid})
      when transaction_owner === nil ->
        {:ok, state} = transact_begin(state, client_pid)
        reply(from, @confirm)
        main_loop(state)

      rcall(from, :commit)
      when transaction_owner === from_pid(from) ->
        state =
          state
          |> transact_commit()
          |> uwok!
          |> save_to_disk()

        reply(from, @confirm)
        main_loop(state)

      rcall(from, :rollback)
      when transaction_owner === from_pid(from) ->
        {:ok, state} = transact_rollback(state)
        reply(from, @confirm)
        Logger.warn("Transaction rollback for #{inspect(from_pid(from))}")
        main_loop(state)

      rcall(from, :shutdown)
      when transaction_owner === nil ->
        Registry.unregister(@registry, state.name)
        reply(from, :ok)
        # ---------------------------- NO LOOP ---------------------
        :ok

      rcall(from, :nuke_storage)
      when transaction_owner === nil ->
        reply(from, nuke_storage(state))
        main_loop(state)

      rcast(:print_dump) ->
        print_dump(state)
        main_loop(state)

      rcall(from, :get_state) ->
        reply(from, state)
        main_loop(state)

      rcall(from, other)
      when transaction_owner in [nil, from_pid(from)] ->
        Logger.warn(
          "Database received unexpected call #{inspect(other)} from #{inspect(from_pid(from))}"
        )

        reply(from, {:error, {:unhandled_message, other}})
        main_loop(state)

      rcast(other) ->
        Logger.warn("Database received unexpected cast #{inspect(other)}")
        main_loop(state)

        # Here is no catchall call because we do not want to match on any
        # message when a transaction is running. This way, those messages will
        # wait in the mailbox that the transaction is over
    end
  end

  defp print_dump(state) do
    ~M(name, storage) = state
    IO.puts("Dump store #{name} : #{inspect(storage)}")
  end

  defp transact_begin(state, client_pid) do
    # we will keep the current data in a safe place, and store the monitor ref
    # for the client to listen to DOWN messages.
    mref = Process.monitor(client_pid)
    ~M(storage) = state

    state =
      state
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
      state
      | transaction_owner: nil,
        transaction_monitor: nil,
        backup: nil,
        storage: storage
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

  defp run_command(state, {:kv_put, key, value}) do
    state
    |> Map.update!(:storage, &Map.put(&1, key, value))
    |> mark_tainted(key)
    |> wok()
  end

  defp run_command(state, {:kv_delete, key}) do
    state
    |> Map.update!(:storage, &Map.delete(&1, key))
    |> mark_deleted(key)
    |> wok()
  end

  defp run_command(state, {:kv_read, afun, args}) do
    {:reply, apply(Map, afun, [state.storage | args])}
  end

  defp run_command(state, command) do
    raise "Unknown command #{inspect(command)}"
  end

  defp mark_tainted(state, key) do
    Map.update!(state, :tainted, fn list -> [key | list] end)
  end

  defp mark_deleted(state, key) do
    Map.update!(state, :deleted, fn list -> [key | list] end)
  end

  defp storage_tainted?(%{tainted: [], deleted: []}), do: false
  defp storage_tainted?(_), do: true

  defp maybe_save_dirty(state, command) do
    IO.puts("@todo maybe_save_dirty #{inspect(command)}")
    state
  end

  defp save_to_disk(state) do
    IO.puts("@todo save_to_disk")
    state
  end

  defp nuke_storage(state) do
    IO.puts("@todo nuke_storage")
    state
  end
end
