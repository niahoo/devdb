defmodule Kvern do
  alias Kvern.Store
  use TODO
  import Store, only: [send_command: 2]
  use Unsafe.Generator, docs: false
  defdelegate begin(db), to: Store
  defdelegate commit(db), to: Store
  defdelegate rollback(db), to: Store

  @type t_update :: {:put, key :: any(), value :: any()} | {:delete, key :: any()}

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
    opts =
      opts
      |> Keyword.put(:name, name)
      |> setup_disk_copy()

    Supervisor.start_child(Kvern.StoreSupervisor, [opts])
  end

  def setup_disk_copy(opts) do
    if opts[:disk_copy] do
      dir = opts[:disk_copy][:dir]
      codec = opts[:disk_copy][:codec]

      # We use a disk repo as a seed
      seed = Kvern.Seed.new(Kvern.Repo.Disk, dir: dir, codec: codec)
      replicate = {Kvern.Repo.Disk, dir: dir, codec: codec}

      opts
      |> Keyword.update(:seeds, [seed], fn seeds -> seeds ++ [seed] end)
      |> Keyword.update(:replicates, [replicate], fn replicates -> replicates ++ [replicate] end)
      |> Keyword.delete(:disk_copy)
    else
      opts
    end
  end

  def whereis(name), do: Store.whereis(name)

  def put(db, key, value) do
    send_command(db, {:kv_put, key, value})
  end

  def get(db, key, default \\ nil) do
    send_command(db, {:kv_read, :get, [key, default]})
  end

  def get_lazy(db, key, fun) do
    # fun is executed in the calling process. As we may or not be in a
    # transaction, we cannot automatically put the value in the store
    case fetch(db, key) do
      :error -> fun.()
      {:ok, val} -> val
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

  def nuke(db) do
    GenLoop.call(Store.via(db), :nuke)
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

      def get_lazy(key, fun), do: Kvern.get_lazy(unquote(name), key, fun)

      def fetch(key), do: Kvern.fetch(unquote(name), key)

      def fetch!(key), do: Kvern.fetch!(unquote(name), key)

      def keys(), do: Kvern.keys(unquote(name))

      def print_dump(), do: Kvern.print_dump(unquote(name))

      def tainted(), do: Kvern.tainted(unquote(name))

      def nuke(), do: Kvern.nuke(unquote(name))

      def shutdown(), do: Kvern.shutdown(unquote(name))
    end
  end
end
