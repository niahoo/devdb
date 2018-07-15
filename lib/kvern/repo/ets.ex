defmodule Kvern.Repo.Ets do
  alias :ets, as: Ets
  use TODO

  @behaviour Kvern.Repo

  defstruct tab: nil

  @todo "Allow ETS configuration or even provide the table"
  def new(opts \\ []) do
    tab = Ets.new(__MODULE__, opts[:ets] || [:set, :protected])
    tab
  end

  def put(tab, key, value) do
    IO.puts("perform ETS insert of #{key} = #{inspect(value)}")

    case Ets.insert(tab, [{key, value}]) do
      true ->
        tab
    end
  end

  @todo "Remove put_as_side_effect! as we can return new repo from Repo.fetch"
  def put_as_side_effect!(tab, key, value), do: put(tab, key, value)

  def fetch(tab, key) do
    case Ets.lookup(tab, key) do
      [{^key, found}] ->
        {:ok, found}

      _else ->
        :error
    end
  end

  def delete(tab, key) do
    true = Ets.delete(tab, key)
    tab
  end

  @todo """
  We must keep not only the tab but also the options in a state in order to
  create a new table
  """
  def nuke(tab) do
    true = Ets.delete(tab)
    __MODULE__.new([])
  end

  def keys(tab) do
    IO.puts("fetching keys for tab #{inspect(tab)}")
    first = Ets.first(tab)
    ks = keys(tab, first, [])
    IO.puts("found keys : #{inspect(ks)}")
    ks
  end

  def keys(_tab, :"$end_of_table", acc), do: :lists.reverse(acc)

  def keys(tab, prev, acc) do
    next = Ets.next(tab, prev)
    keys(tab, next, [prev | acc])
  end

  def transactional(tab) do
    {:ok, {Kvern.Repo.TransactionalETS, [tab: tab]}}
  end
end
