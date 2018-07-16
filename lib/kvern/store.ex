defmodule Kvern.Store do
  require Logger
  import ShorterMaps
  use GenLoop, enter: :main_loop
  alias Kvern.Repo

  defmodule S do
    defstruct name: nil,
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

  def open(name, opts \\ []) when is_atom(name) do
    # At the moment, we will force the shape of the different storages on the
    # server :

    # The basic store is ETS

    # If the option :disk_copy is given (and is a valid directory), the ETS
    # store will be backed by a Disk store. The ETS store will be configured to
    # be warmed-up, i.e. it will import everything from the Disk store.

    # On top of all this, during transactions we will add a TransactionalETS
    # layer that will read the same table as the ETS store but will keep log of
    # modifications to be able to rollback. The ETS table belongs to the store,
    # so if a problem happens in a transaction, the store should crash and the
    # ETS table will be deleted, so no problem of with failed rollbacks

    IO.puts("open with opts #{inspect(opts)}")

    repo =
      if opts[:disk_copy] do
        ets_with_disk_backend(opts)
      else
        default_ets_repo(opts)
      end

    opts = [name: name, repo: repo]

    Supervisor.start_child(Kvern.StoreSupervisor, [opts])
  end

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

  def transaction(db, fun) do
    case fun.() do
      {:ok, reply} ->
        commit(db)
        {:ok, reply}

      atom when atom in [:commit, :ok] ->
        commit(db)
        :ok

      atom when atom in [:rollback, :error] ->
        rollback(db)
        :error

      {:error, error} ->
        rollback(db)
        {:error, error}
    end
  rescue
    e ->
      rollback(db)
      {:error, e}
  catch
    :throw, e ->
      rollback(db)
      {:error, e}
  end

  def get_state(db) do
    GenLoop.call(via(db), :get_state)
  end

  defp ets_with_disk_backend(opts) do
    disk_repo = {Kvern.Repo.Disk, dir: opts[:disk_copy], codec: opts[:codec]}

    backend = [repo: disk_repo, read: true, write: true, warmup: true]
    default_ets_repo([{:backend, backend} | opts])
  end

  defp default_ets_repo(opts) do
    module = Repo.Ets
    take = [:ets, :backend]
    {options, other} = Keyword.split(opts, take)
    Logger.debug("Ignoring options for ETS repo #{inspect(Keyword.keys(other))}")

    {module, options}
  end

  def whereis(name), do: Registry.whereis_name({@registry, name})

  defp uwok!({:ok, val}), do: val

  defp wok({:error, _} = err), do: err
  defp wok(:ok), do: :ok
  defp wok({:ok, val}), do: {:ok, val}
  defp wok(val), do: {:ok, val}

  # -- Database initialization ------------------------------------------------

  def init(opts) do
    {repo_module, repo_conf} = Keyword.fetch!(opts, :repo)
    repo = Repo.new(repo_module, repo_conf)

    state = %S{
      name: opts[:name],
      repo: repo
    }

    state =
      if opts[:seeds] do
        Logger.debug("Applying seed")
        s = Enum.reduce(opts[:seeds], state, &apply_seed/2)
        Logger.debug("Seeding ok")
        s
      else
        state
      end

    {:ok, state}
  end

  def data_vsn(), do: 5

  def code_change(_oldvsn, state, _extra) do
    {:ok, {:newstate, state}}
  end

  # -- Server States ----------------------------------------------------------

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

          _other ->
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

      rcall(from, :tainted?) ->
        reply(from, repo_tainted?(state))
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

  defp print_dump(~M(name, repo)) do
    dump = "STORE DUMP -------------------\nSTORE #{name}\n\n"

    repo
    |> Repo.keys()
    |> Enum.reduce(dump, fn key, dump ->
      value = Repo.fetch!(repo, key)
      # I have to do this syntax so the formatter do not mess with me
      dump = dump <> ":  #{key}\n"
      dump = dump <> "   #{inspect(value, pretty: true)}\n"
      dump
    end)
    |> Kernel.<>("END DUMP ---------------------\n")
    |> IO.puts()
  end

  defp transact_begin(state, client_pid) do
    # we will keep the current data in a safe place, and store the monitor ref
    # for the client to listen to DOWN messages.
    mref = Process.monitor(client_pid)
    ~M(repo) = state

    transactional_repo = Repo.transactional(repo)

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
    state
    |> Map.put(:repo, Repo.rollback(state.repo))
    |> transact_cleanup(state.backup)
  end

  defp transact_commit(state) do
    # set the old repo back in state.repo
    {:ok, _new_transact_repo, updates} = Repo.commit(state.repo)
    repo = Repo.apply_updates(state.backup, updates)
    transact_cleanup(state, repo)
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
        Logger.error("#{msg}\n(stacktrace) #{inspect(System.stacktrace(), pretty: true)}")
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

  defp run_command(_state, command) do
    raise "Unknown command #{inspect(command)}"
  end

  defp mark_tainted(state = %{transaction_owner: nil}, _key), do: state

  defp mark_tainted(state, key) do
    Map.update!(state, :tainted, fn list -> [key | list] end)
  end

  defp mark_deleted(state = %{transaction_owner: nil}, _key), do: state

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

  defp nuke(state) do
    state
    |> Map.put(:repo, Repo.nuke(state.repo))
    |> wok()
  end

  defp apply_seed(seed, state) do
    # We require the seed to provide a stream but at the moment we just turn
    # this into an enum.
    updates = Kvern.Seed.stream_updates(seed)
    updates = Enum.to_list(updates)
    repo = Repo.apply_updates(state.repo, updates)
    %S{state | repo: repo}
  end
end
