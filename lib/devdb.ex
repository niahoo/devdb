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
    repo = make_repo(opts)
    apply(SingleLock, start_fun, [opts])
  end

  def stop(db, reason \\ :normal, timeout \\ :infinity) do
    SingleLock.stop(db, reason, timeout)
  end

  ## -- --

  defp make_repo(opts) do
  end
end
