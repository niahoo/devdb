defmodule Kvern.Repo do
  use TODO
  @type repo_state :: any()

  @callback new(opts :: [any()]) :: any()
  @callback delete(repo_state(), key :: any()) :: repo_state()
  @callback fetch(repo_state(), key :: any()) :: {:ok, any()} | {:error, any()}
  @callback keys(repo_state()) :: [any()]
  @callback nuke(repo_state()) :: repo_state()
  @callback put(repo_state(), key :: any(), value :: any()) :: repo_state()
  @callback transactional(repo_state()) ::
              {:ok, {module :: atom(), [any()]}} | {:error, :unsupported}

  @todo "Enable transactional callbacks"
  @todo "Use protocols ?"
  # @callback rollback(repo_state()) :: repo_state()

  defstruct mod: nil, state: nil, backend: nil

  @m __MODULE__

  @type t :: %__MODULE__{}

  require Record
  # beconf = backend configuration
  Record.defrecord(:beconf, repo: nil, read: false, write: false, warmup: false)

  @type beconf ::
          record(
            :beconf,
            repo: __MODULE__.t() | nil,
            read: boolean(),
            write: boolean(),
            warmup: boolean()
          )
  # expands to: "@type user :: {:user, String.t, integer}"

  def new(mod, opts \\ []) do
    backend = make_backend(opts[:backend])
    %@m{mod: mod, state: mod.new(opts), backend: backend}
  end

  def put(repo = %@m{mod: mod, state: state, backend: beconf(write: true) = bconf}, key, value) do
    IO.puts("#{print_mod(mod)}> PUT #{key} * : #{inspect(value)}")

    # It's important to update the store before the backend if the store is in
    # transactional mode and want to backup the data. ETS and TransactionalETS
    # work on the same table.
    todo("""
    Use records in ETS to store value and transactional value on
    different fields ?
    """)

    new_state = mod.put(state, key, value)
    new_backend = put_in_backend(bconf, key, value)
    %{repo | state: new_state, backend: new_backend}
  end

  def put(repo = %@m{mod: mod, state: state}, key, value) do
    IO.puts("#{print_mod(mod)}> PUT #{key} : #{inspect(value)}")
    %{repo | state: mod.put(state, key, value)}
  end

  def transactional(%@m{mod: mod, state: state}) do
    {:ok, {new_mod, options}} = mod.transactional(state)
    __MODULE__.new(new_mod, [{:transaction_for, mod} | options])
  end

  def put_in_backend(conf = beconf(write: true, repo: backend), key, value) do
    new_backend = __MODULE__.put(backend, key, value)
    beconf(conf, repo: new_backend)
  end

  def delete(repo = %@m{mod: mod, state: state, backend: beconf(write: true) = bconf}, key) do
    IO.puts("#{print_mod(mod)}> DELETE #{key} *")
    # Here we must delete on the store before doing so on the backend for
    # transactions. see put/3
    new_state = mod.delete(state, key)
    new_backend = delete_in_backend(bconf, key)
    %{repo | state: new_state, backend: new_backend}
  end

  def delete(repo = %@m{mod: mod, state: state}, key) do
    IO.puts("#{print_mod(mod)}> DELETE #{key}")
    %{repo | state: mod.delete(state, key)}
  end

  def delete_in_backend(conf = beconf(write: true, repo: backend), key) do
    new_backend = __MODULE__.delete(backend, key)
    beconf(conf, repo: new_backend)
  end

  def nuke(repo = %@m{mod: mod, state: state}) do
    IO.puts("#{print_mod(mod)}> NUKE")
    %{repo | state: mod.nuke(state)}
  end

  @todo """
  The repo is updated with the fallback value. If we want to use permanent data
  structures like maps, we also must return the new repo after fetch.
  """
  def fetch(%@m{mod: mod, state: state, backend: backend}, key) do
    IO.puts("#{print_mod(mod)}> FETCH #{key}")

    case {mod.fetch(state, key), backend} do
      {{:ok, found}, _} ->
        {:ok, found}

      {:error, beconf(repo: be_repo, read: true)} ->
        IO.puts("#{print_mod(mod)}> FETCH FALLBACK #{key} <= #{print_mod(be_repo.mod)}")

        case fetch(be_repo, key) do
          {:ok, val} ->
            mod.put_as_side_effect!(state, key, val)
            {:ok, val}

          :error ->
            :error
        end

      # no fallback to get the data
      {:error, _} ->
        :error
    end
  end

  def fetch!(%@m{mod: mod, state: state}, key) do
    IO.puts("#{print_mod(mod)}> FETCH! #{key}")
    unwrap_fetch(mod.fetch(state, key), mod, key)
  end

  def unwrap_fetch({:ok, val}, _, _), do: val
  def unwrap_fetch(:error, mod, key), do: raise(KeyError, key: key, term: {__MODULE__, mod})

  def keys(%@m{mod: mod, state: state}) do
    IO.puts("#{print_mod(mod)}> KEYS")
    mod.keys(state)
  end

  def get(repo, key, default \\ nil) do
    case fetch(repo, key) do
      {:ok, found} ->
        found

      :error ->
        default
    end
  end

  def apply_updates(repo, updates), do: Enum.reduce(updates, repo, &apply_up/2)

  def apply_up({:put, key, val}, repo), do: @m.put(repo, key, val)
  def apply_up({:delete, key}, repo), do: @m.delete(repo, key)

  def set_backend(repo = %@m{}, backend = beconf()) do
    %@m{repo | backend: backend}
  end

  def commit(_repo = %@m{}) do
    :ok
  end

  def rollback(repo = %@m{mod: mod, state: state}) do
    IO.puts("#{print_mod(mod)}> ROLLBACK")

    %{repo | state: mod.rollback(state)}
  end

  defp make_backend(nil), do: nil

  defp make_backend(opts) do
    {mod, conf} = Keyword.fetch!(opts, :repo)
    repo = __MODULE__.new(mod, conf)
    write = Keyword.fetch!(opts, :write)
    read = Keyword.fetch!(opts, :read)
    warmup = Keyword.fetch!(opts, :warmup)

    beconf(repo: repo, write: write, read: read, warmup: warmup)
  end

  defp print_mod(module) do
    Process.sleep(100)

    module
    |> Module.split()
    |> Enum.drop(2)
    |> Enum.join()
    |> String.pad_trailing(16)
  end
end
