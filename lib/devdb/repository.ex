defprotocol DevDB.Repository do
  def put(repo, key, value)
  def delete(repo, key)
  def fetch(repo, key)
  def select(repo, filter)
  def get_commit_updates(repo)
  def begin_transaction(repo)
  def commit_transaction(repo)
  def rollback_transaction(repo)
end

# defmodule DevDB.Repo do
#   defdelegate put(repo, key, value), to: DevDB.Repository
#   defdelegate fetch(repo, key), to: DevDB.Repository
#   defdelegate select(repo, filter), to: DevDB.Repository
#   defdelegate delete(repo, key), to: DevDB.Repository
#   defdelegate get_commit_updates(repo), to: DevDB.Repository
#   defdelegate begin_transaction(repo), to: DevDB.Repository
#   defdelegate commit_transaction(repo), to: DevDB.Repository
#   defdelegate rollback_transaction(repo), to: DevDB.Repository
# end
