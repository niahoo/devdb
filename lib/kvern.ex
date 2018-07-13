defmodule Kvern do
  alias Kvern.Store
  import Store, only: [send_command: 2]
  use Unsafe.Generator, docs: false
  defdelegate begin(db), to: Store
  defdelegate commit(db), to: Store
  defdelegate rollback(db), to: Store

  @unsafe [
    {
      :put,
      3,
      :unwrap_put
    }
    # {
    #   :fetch,
    #   2,
    #   :unwrap_fetch
    # }
  ]

  @key_maxlen 50

  def key_maxlen, do: @key_maxlen

  def open(name, opts \\ []) when is_atom(name) do
    opts = opts |> Keyword.put(:name, name)
    Supervisor.start_child(Kvern.StoreSupervisor, [opts])
  end

  def whereis(name), do: Store.whereis(name)

  def put(db, key, value) do
    send_command(db, {:kv_put, key, value})
  end

  def get(db, key, default \\ nil) do
    send_command(db, {:kv_read, :get, [key, default]})
  end

  def get_lazy(db, key, fun) do
    begin(db)
    # fun must be executed in the calling process
    case get(db, key, {:not_found, key}) do
      {:not_found, ^key} ->
        try do
          val = fun.()
          put!(db, key, val)
          commit(db)
          val
        rescue
          e ->
            stacktrace = System.stacktrace()
            rollback(db)
            reraise(e, stacktrace)
        end

      val ->
        val
    end
  end

  def fetch(db, key) do
    send_command(db, {:kv_read, :fetch, [key]})
  end

  def fetch!(db, key), do: unwrap_fetch(fetch(db, key), db, key)

  def keys(db) do
    send_command(db, {:kv_read, :keys, []})
  end

  def delete(db, key) do
    send_command(db, {:kv_delete, key})
  end

  def print_dump(db) do
    GenLoop.cast(Store.via(db), :print_dump)
  end

  def tainted(db) do
    GenLoop.call(Store.via(db), :tainted)
  end

  def nuke_storage(db) do
    GenLoop.call(Store.via(db), :nuke_storage)
  end

  def shutdown(db) do
    GenLoop.call(Store.via(db), :shutdown)
  end

  def unwrap_put(:ok), do: :ok
  def unwrap_put({:error, reason}), do: raise("could not put #{inspect(reason)}")

  def unwrap_fetch({:ok, val}, _, _), do: val
  def unwrap_fetch(:error, db, key), do: raise(KeyError, key: key, term: {__MODULE__, db})

  # Defines a simple macro that hardcode the store name in the calls to the API
  # @optimize : this doubles every function call ...
  @todo "accept a callback to transform the keys"
  defmacro __using__(opts) do
    name = Keyword.fetch!(opts, :name)

    quote do
      def put(key, value), do: Kvern.put(unquote(name), key, value)

      def put!(key, value), do: Kvern.put!(unquote(name), key, value)

      def get(key, default \\ nil), do: Kvern.get(unquote(name), key, default)

      def get_lazy(key, fun), do: Kvern.get(unquote(name), key, fun)

      def fetch(key), do: Kvern.fetch(unquote(name), key)

      def fetch!(key), do: Kvern.fetch!(unquote(name), key)

      def keys(), do: Kvern.keys(unquote(name))

      def print_dump(), do: Kvern.print_dump(unquote(name))

      def tainted(), do: Kvern.tainted(unquote(name))

      def nuke_storage(), do: Kvern.nuke_storage(unquote(name))

      def shutdown(), do: Kvern.shutdown(unquote(name))
    end
  end
end
