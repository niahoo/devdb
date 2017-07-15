defmodule Kvern do
  alias Kvern.Store
  import Store, only: [send_command: 2]
  defdelegate begin(db), to: Store
  defdelegate commit(db), to: Store
  defdelegate rollback(db), to: Store

  @key_maxlen 30

  @doc """
  The key/value stores backups data to disk. To be sure that the filename will
  be acceptable on any playform, we set very strict rules:
  - Only letters, numbers, dashes and underscores
  - Starts with a letter
  - Max lenght 30 (byte length)
  """
  def valid_key?(key) when not is_binary(key),
    do: {:error, {:bad_key, :not_binary}}

  def valid_key?(key) when byte_size(key) > @key_maxlen,
    do: {:error, {:bad_key, :too_long}}

  def valid_key?(key) do
    ~r/^[a-zA-Z]+[0-9a-zA-Z_-]*$/
    |> Regex.match?(key)
    |> if(do: :ok, else: {:error, {:bad_key, :bad_characters}})
  end

  def open(name) when is_atom(name),
    do: open([name: name])
  def open(config) when is_list(config) do
    {:ok, _pid} = Supervisor.start_child(Kvern.StoreSupervisor, [config])
  end

  def put(db, key, value) do
    with :ok <- valid_key?(key) do
      send_command(db, {:kv_put, key, value})
    end
  end

  def put!(db, key, value) do
    :ok = put(db, key, value)
  end

  def get(db, key, default \\ nil) do
    case send_command(db, {:kv_get, key}) do
      nil -> default
      value -> value
    end
  end

  def fetch(db, key) do
    send_command(db, {:kv_fetch, key})
  end

  def print_dump(db) do
    GenServer.cast(Store.via(db), :print_dump)
  end

end
