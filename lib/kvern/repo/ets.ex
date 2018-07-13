defmodule Kvern.Repo.Ets do
  alias :ets, as: Ets

  defstruct [:tab]

  def new() do
    tab = Ets.new(__MODULE__, [:set, :private])
    tab
  end

  def put(tab, key, value) do
    case Ets.insert(tab, [{key, value}]) do
      true ->
        tab
    end
  end

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

  def keys(tab) do
    first = Ets.first(tab)
    keys(tab, first, [first])
  end

  def keys(_tab, :"$end_of_table", acc), do: :lists.reverse(acc)

  def keys(tab, prev, acc) do
    next = Ets.next(tab, prev)
    keys(tab, next, [next | acc])
  end
end
