defprotocol DevDB.Repository do
  def put(repo, key, value)
  def fetch(repo, key)
end

defmodule DevDB.Repo do
  def put(repo, key, value) do
    DevDB.Repository.put(repo, key, value)
  end
  def fetch(repo, key) do
    DevDB.Repository.fetch(repo, key)
  end
end
