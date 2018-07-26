defprotocol DevDB.Repository do
  def put(repo, key, value)
  def delete(repo, key)
  def fetch(repo, key)
  def select(repo, filter)
  def begin_transaction(repo)
  def commit_transaction(repo)
  def rollback_transaction(repo)
end

defmodule DevDB.Repo do
  defstruct state: nil

  def new(module, opts) do
    state = module.new(opts)
    %__MODULE__{state: state}
  end

  def put(repo, key, value), do: DevDB.Repository.put(repo.state, key, value)
  def fetch(repo, key), do: DevDB.Repository.fetch(repo.state, key)

  def select(this, filter) when is_function(filter, 1) do
    filter_without_key = fn val, _key ->
      IO.puts("filter_without_key, key: #{inspect(_key)}, val: #{inspect(val)}")
      filter.(val)
    end

    select(this, filter_without_key)
  end

  def select(repo, filter), do: DevDB.Repository.select(repo.state, filter)

  def delete(repo, key), do: DevDB.Repository.delete(repo.state, key)

  def begin_transaction(repo) do
    {:ok, new_state} = DevDB.Repository.begin_transaction(repo.state)
    {:ok, Map.put(repo, :state, new_state)}
  end

  def commit_transaction(repo) do
    {:ok, new_state} = DevDB.Repository.commit_transaction(repo.state)
    {:ok, Map.put(repo, :state, new_state)}
  end

  def rollback_transaction(repo) do
    {:ok, new_state} = DevDB.Repository.rollback_transaction(repo.state)
    {:ok, Map.put(repo, :state, new_state)}
  end
end
