defprotocol DevDB.Repository do
  def put(repo, key, value)
  def delete(repo, key)
  def fetch(repo, key)
end

defmodule DevDB.Repo do
  def put(repo, key, value) do
    DevDB.Repository.put(repo, key, value)
  end

  def fetch(repo, key) do
    DevDB.Repository.fetch(repo, key)
  end

  def delete(repo, key) do
    DevDB.Repository.delete(repo, key)
  end
end
