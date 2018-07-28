defmodule DevDB do
  require Logger
  alias DevDB.Repository, as: Repo

  def start_link(name, opts \\ []) do
    start(:start_link, name, opts)
  end

  def start(name, opts \\ []) do
    start(:start, name, opts)
  end

  defp start(start_fun, name, opts) do
    # We give the repository to the ets broker so it can be sent to every
    # client.

    # We are ETS aware : despite the repository is implemented using a protocol
    # for practical reasons (test and dev), we know that the repository type
    # here is a DevDB.Repository.Ets. So we use it to create the table.
    create_table = fn -> DevDB.Repository.Ets.create_table(name, [:private]) end

    # As metadata we give the module we want to use with the ets table, its
    # specific options, and the options for the repository like backend,
    {repo_opts, opts} = Keyword.split(opts, [:backend])
    repository = DevDB.Repository.new(repo_opts)

    broker_opts = [
      meta: repository,
      create_table: create_table,
      name: name
    ]

    apply(EtsBroker, start_fun, [broker_opts])
  end

  def stop(db, reason \\ :normal, timeout \\ :infinity) do
    EtsBroker.stop(db, reason, timeout)
  end

  ## -- --

  def put(db, key, value) when is_pid(db) or is_atom(db) do
    fail_if_in_transaction(fn ->
      single_update_command(db, fn repo -> Repo.put(repo, key, value) end)
    end)
  end

  def put({:tr_repo, repo}, key, value) do
    Repo.tr_put(repo, key, value)
  end

  def delete(db, key) when is_pid(db) or is_atom(db) do
    fail_if_in_transaction(fn ->
      single_update_command(db, fn repo -> Repo.delete(repo, key) end)
    end)
  end

  def delete({:tr_repo, repo}, key) do
    Repo.tr_delete(repo, key)
  end

  def fetch(db, key) when is_pid(db) or is_atom(db) do
    fail_if_in_transaction(fn ->
      single_read_command(db, fn repo -> Repo.fetch(repo, key) end)
    end)
  end

  def fetch({:tr_repo, repo}, key) do
    Repo.tr_fetch(repo, key)
  end

  def select(db, filter)
      when (is_pid(db) or is_atom(db)) and (is_function(filter, 2) or is_function(filter, 1)) do
    fail_if_in_transaction(fn ->
      single_read_command(db, fn repo -> Repo.select(repo, filter) end)
    end)
  end

  def select({:tr_repo, repo}, filter) do
    Repo.tr_select(repo, filter)
  end

  def transaction(db, fun) when is_pid(db) or is_atom(db) do
    fail_if_in_transaction(fn ->
      call_with_repo(db, fn base_repo ->
        {:ok, tr_repo} = Repo.begin_transaction(base_repo)

        # We wrap the transactional repo in a :tr_repo tagged tuple so we can
        # easily pattern match on the single API functions put/fetch/...

        # When the transaction is over, the transactional repo is just ditched
        # and the base_repo remains unchanged.

        IO.puts("BEGIN")

        case fun.({:tr_repo, tr_repo}) do
          {:ok, _} = reply ->
            IO.puts("COMMIT")
            commit_transaction(tr_repo, reply)

          ok_atom when ok_atom in [:ok, :commit] ->
            IO.puts("COMMIT")
            commit_transaction(tr_repo, :ok)

          {:error, _} = err ->
            IO.puts("ROLLBACK")
            rollback_transaction(tr_repo, err)

          error_atom when error_atom in [:error, :rollback] ->
            IO.puts("ROLLBACK")
            rollback_transaction(tr_repo, :error)
        end
      end)
    end)
  end

  ## -- --

  # Here we use only functions from this module, that we know are one-op and act
  # on the default ETS repo (non-transactional)
  defp single_update_command(db, fun) when is_pid(db) or is_atom(db) do
    call_with_repo(db, fn repo ->
      case fun.(repo) do
        :ok -> {:reply, :ok}
        {:error, _} = err -> err
      end
    end)
  end

  defp single_read_command(db, fun) when is_pid(db) or is_atom(db) do
    call_with_repo(db, fn repo ->
      case fun.(repo) do
        {:ok, data} -> {:reply, {:ok, data}}
        {:error, _} = err -> err
        :error -> :error
      end
    end)
  end

  @todo "here give the backend to apply updates too. use replicates or pubsub."
  defp commit_transaction(tr_repo, reply) do
    case Repo.commit_transaction(tr_repo) do
      :ok ->
        {:reply, reply}

      {:error, err} ->
        raise "Could not commit the transaction, err: #{inspect(err)}"
    end
  end

  defp rollback_transaction(tr_repo, reply) do
    Logger.error("Rollback transaction : #{inspect(reply)}")

    case Repo.rollback_transaction(tr_repo) do
      :ok ->
        {:reply, reply}

      {:error, err} ->
        raise "Could not commit the transaction, err: #{inspect(err)}"
    end
  end

  defp call_with_repo(db, fun) do
    EtsBroker.borrow(db, fn tab, repo ->
      repo = Repo.set_main_store(repo, Repo.Ets.new(tab))

      case fun.(repo) do
        {:reply, reply} ->
          reply

        {:error, _} = err ->
          err

        :error = err ->
          :error
      end
    end)
  end

  @tr_check {__MODULE__, :in_transaction}

  defp fail_if_in_transaction(fun) do
    if Process.get(@tr_check) === true do
      raise DevDB.Error,
        message: "Cannot invoke DevDB.functions on pid/atom while in transaction."
    end

    Process.put(@tr_check, true)

    try do
      _return = fun.()
    after
      Process.delete(@tr_check)
    end
  end
end
