defmodule Kvern.Store do
  require Logger
  import ShorterMaps
  use GenLoop, enter: :main_loop
  alias Kvern.Repo

  defmodule S do
    defstruct name: nil,
              config: nil,
              transaction_owner: nil,
              transaction_monitor: nil,
              # copy of the default repo during transactions
              backup: nil,
              # default repository, and transactional repository during transactions
              repo: nil,
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

    state = %S{
      config: opts,
      name: name,
      repo: Repo.new(Kvern.Repo.Ets)
    }

    IO.puts("Initializing store #{inspect(state, pretty: true)}")
    IO.puts("@todo seed repo with provided seeders")

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
            reply(from, reply)
            main_loop(new_state)

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

      rcall(from, :nuke) ->
        case transaction_owner do
          nil ->
            new_state = state |> nuke |> uwok!
            reply(from, :ok)
            main_loop(new_state)

          other ->
            reply(from, {:error, :in_transaction})
            main_loop(state)
        end

      rcast(:print_dump) ->
        print_dump(state)
        main_loop(state)

      rcall(from, :get_state) ->
        reply(from, state)
        main_loop(state)

      rcall(from, :tainted) ->
        reply(from, state.tainted)
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
    ~M(name, repo) = state
    IO.puts("Dump store #{name} : #{inspect(repo)}")
  end

  defp transact_begin(state, client_pid) do
    # we will keep the current data in a safe place, and store the monitor ref
    # for the client to listen to DOWN messages.
    mref = Process.monitor(client_pid)
    ~M(repo) = state

    transactional_repo = Repo.new(Kvern.Repo.Transactional, read_fallback: repo)

    state =
      state
      |> Map.put(:transaction_owner, client_pid)
      |> Map.put(:transaction_monitor, mref)
      |> Map.put(:backup, repo)
      |> Map.put(:repo, transactional_repo)

    {:ok, state}
  end

  defp transact_rollback(%{transaction_owner: nil} = state) do
    # Not in transaction, cannot rollback
    raise "@todo does this even happen ?"
    {:ok, state}
  end

  defp transact_rollback(state) do
    # set the old repo back in state.repo
    transact_cleanup(state, state.backup)
  end

  defp transact_commit(state) do
    # set the old repo back in state.repo
    updates = tainted_to_updates(state)
    repo = Repo.apply_updates(state.backup, updates)
    transact_cleanup(state, repo)
  end

  defp tainted_to_updates(state) do
    %{tainted: tainted, deleted: deleted, repo: repo} = state
    tainted_updates = Enum.map(tainted, fn key -> {:put, key, Repo.fetch!(repo, key)} end)
    deleted_updates = Enum.map(deleted, fn key -> {:delete, key} end)
    tainted_updates ++ deleted_updates
  end

  defp transact_cleanup(state, repo) do
    # put the provided repo in the state
    # demonitor transaction client
    # set transaction informations to nil
    Process.demonitor(state.transaction_monitor, [:flush])

    state = %S{
      state
      | transaction_owner: nil,
        transaction_monitor: nil,
        backup: nil,
        repo: repo,
        tainted: [],
        deleted: []
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
      # {:ok, reply, new_state} ->
      # {:continue, reply, new_state}

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
    |> Map.put(:repo, Repo.put(state.repo, key, value))
    |> mark_tainted(key)
    |> unmark_deleted(key)
    |> wok()
  end

  defp run_command(state, {:kv_delete, key}) do
    state
    |> Map.put(:repo, Repo.delete(state.repo, key))
    |> mark_deleted(key)
    |> unmark_tainted(key)
    |> wok()
  end

  defp run_command(state, {:kv_read, read_fun, args}) do
    {:reply, apply(Repo, read_fun, [state.repo | args])}
  end

  defp run_command(state, command) do
    raise "Unknown command #{inspect(command)}"
  end

  defp mark_tainted(%{transaction_owner: nil} = state, key), do: state

  defp mark_tainted(state, key) do
    Map.update!(state, :tainted, fn list -> [key | list] end)
  end

  defp mark_deleted(%{transaction_owner: nil} = state, key), do: state

  defp mark_deleted(state, key) do
    Map.update!(state, :deleted, fn list -> [key | list] end)
  end

  defp unmark_deleted([], _), do: []
  defp unmark_deleted([key | keys], key), do: unmark_deleted(keys, key)
  defp unmark_deleted([other | keys], key), do: [other | unmark_deleted(keys, key)]

  defp unmark_deleted(%{deleted: deleted} = state, key) do
    deleted = unmark_deleted(deleted, key)
    Map.put(state, :deleted, deleted)
  end

  defp unmark_tainted([], _), do: []
  defp unmark_tainted([key | keys], key), do: unmark_tainted(keys, key)
  defp unmark_tainted([other | keys], key), do: [other | unmark_tainted(keys, key)]

  defp unmark_tainted(%{tainted: tainted} = state, key) do
    tainted = unmark_tainted(tainted, key)
    Map.put(state, :tainted, tainted)
  end

  defp repo_tainted?(%{tainted: [], deleted: []}), do: false
  defp repo_tainted?(_), do: true

  defp save_to_disk(state) do
    # IO.puts("@todo save_to_disk")
    state
  end

  defp nuke(state) do
    state
    |> Map.put(:repo, Repo.nuke(state.repo))
    |> wok()
  end
end
