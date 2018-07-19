defmodule DevDB do
  # A database is simply a SingleLock process whose value is a repository
  # configuration with an ETS table. The SingleLock process holds the table.
  # Acquiring the lock is the default way of acting on the table, so this allows
  # to serialize complex operations (like selecting, and updating an entity in
  # an isolated way)

  def start_link(name, opts \\ []) do
    start(:start_link, name, opts)
  end

  def start(name, opts \\ []) do
    start(:start, name, opts)
  end

  defp start(start_fun, name, opts) do
    opts = Keyword.put(opts, :name, name)

    create = fn ->
      make_repo(opts)
    end

    opts = [{:value, create} | opts]

    apply(SingleLock, start_fun, [opts])
  end

  def stop(db, reason \\ :normal, timeout \\ :infinity) do
    SingleLock.stop(db, reason, timeout)
  end

  ## -- --

  alias DevDB.Repo

  def put(db, key, value) when is_pid(db) or is_atom(db) do
    single_update_command(db, fn repo -> Repo.put(repo, key, value) end)
  end

  def delete(db, key) when is_pid(db) or is_atom(db) do
    single_update_command(db, fn repo -> Repo.delete(repo, key) end)
  end

  def fetch(db, key) when is_pid(db) or is_atom(db) do
    single_read_command(db, fn repo -> Repo.fetch(repo, key) end)
  end

  ## -- --

  defp make_repo(opts) do
    DevDB.Repository.Ets.new(opts)
  end

  # Here we use only functions from this module, that we know are one-op and act
  # on the default ETS repo (non-transactional)
  defp single_update_command(db, fun) when is_pid(db) or is_atom(db) do
    safe_single_command(db, fn repo ->
      case fun.(repo) do
        :ok ->
          {:reply, :ok}

        {:ok, updated_repo} ->
          {:reply, :ok, updated_repo}

        {:error, _} = err ->
          err
      end
    end)
  end

  defp single_read_command(db, fun) when is_pid(db) or is_atom(db) do
    safe_single_command(db, fn repo ->
      case fun.(repo) do
        {:ok, data} -> {:reply, {:ok, data}}
        {:error, _} = err -> err
        :error -> :error
      end
    end)
  end

  defp safe_single_command(db, fun) do
    {:ok, repo} = SingleLock.acquire(db)

    case catch_call(fun, [repo]) do
      {:reply, reply} ->
        :ok = SingleLock.release(db)
        reply

      {:reply, reply, new_repo} ->
        :ok = SingleLock.release(db, new_repo)
        reply

      {:error, _} = err ->
        :ok = SingleLock.release(db)
        err

      :error = err ->
        :ok = SingleLock.release(db)
        :error
    end
  end

  defp catch_call(fun, args) do
    apply(fun, args)
  rescue
    e ->
      {:error, Exception.message(e)}
  end
end
