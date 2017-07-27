defmodule Kvern do
  alias Kvern.Store
  import Store, only: [send_command: 2]
  defdelegate begin(db), to: Store
  defdelegate commit(db), to: Store
  defdelegate rollback(db), to: Store

  @key_maxlen 50

  def key_maxlen, do: @key_maxlen

  @doc """
  The key/value stores backups data to disk. To be sure that the filename will
  be acceptable on any playform, we set very strict rules:
  - Only letters, numbers, dots, dashes and underscores
  - Starts with a letter
  - Max lenght 30 (byte length)
  """
  def check_valid_key(key) when not is_binary(key),
    do: {:error, {:bad_key, :not_binary, key}}

  def check_valid_key(key) when byte_size(key) > @key_maxlen,
    do: {:error, {:bad_key, :too_long, key}}

  def check_valid_key(key) do
    ~r/^[a-zA-Z]+[\.0-9a-zA-Z_-]*$/
    |> Regex.match?(key)
    |> if(do: :ok, else: {:error, {:bad_key, :bad_characters, key}})
  end

  def valid_key?(key) do
    case check_valid_key(key) do
      :ok -> true
      _ -> false
    end
  end

  def open(name) when is_atom(name),
    do: open([name: name])
  def open(config) when is_list(config) do
    result = Supervisor.start_child(Kvern.StoreSupervisor, [config])
    IO.puts "Opened Kvern"
         <> "\n\tconfig: #{inspect config}"
         <> "\n\tresult: #{inspect result}"
    result
  end

  def put(db, key, value) do
    with :ok <- check_valid_key(key) do
      send_command(db, {:kv_put, key, value})
      :ok
    end
  end

  def put!(db, key, value) do
    :ok = put(db, key, value)
  end

  def get(db, key, default \\ nil) do
    send_command(db, {:kv_read, :get, [key, default]})
  end

  def get_lazy(db, key, fun) do
    # fun must be executed in the calling process
    case get(db, key, {:not_found, key}) do
      {:not_found, ^key} -> fun.()
      val -> val
    end
  end

  def fetch(db, key) do
    send_command(db, {:kv_read, :fetch, [key]})
  end

  def fetch!(db, key) do
    case fetch(db, key) do
      {:ok, val} -> val
      :error ->
        raise(KeyError, key: key, term: {__MODULE__, db})
    end
  end

  def keys(db) do
    send_command(db, {:kv_read, :keys, []})
  end

  def delete(db, key) do
    send_command(db, {:kv_delete, key})
  end

  def print_dump(db) do
    GenServer.cast(Store.via(db), :print_dump)
  end

  def nuke_storage(db) do
    GenServer.call(Store.via(db), :nuke_storage)
  end

  def shutdown(db) do
    GenServer.call(Store.via(db), :shutdown)
  end

  # Defines a simple macro that hardcode the store name in the calls to the API
  # @optimize : this doubles every function call ...
  defmacro __using__(opts) do
    name = Keyword.fetch!(opts, :name)
    quote do
      def put(key, value),
        do: Kvern.put(unquote(name), key, value)

      def put!(key, value),
        do: Kvern.put!(unquote(name), key, value)

      def get(key, default \\ nil),
        do: Kvern.get(unquote(name), key, default)

      def get_lazy(key, fun),
        do: Kvern.get(unquote(name), key, fun)

      def fetch(key),
        do: Kvern.fetch(unquote(name), key)

      def fetch!(key),
        do: Kvern.fetch!(unquote(name), key)

      def keys(),
        do: Kvern.keys(unquote(name))

      def print_dump(),
        do: Kvern.print_dump(unquote(name))

      def nuke_storage(),
        do: Kvern.nuke_storage(unquote(name))

      def shutdown(),
        do: Kvern.shutdown(unquote(name))
    end
  end

end
