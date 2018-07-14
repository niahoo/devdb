defmodule Kvern.Repo.TransactionalETS do
  use TODO
  @m __MODULE__
  @behaviour Kvern.Repo
  alias Kvern.Repo.Ets, as: BaseETS

  defstruct [:base_state]

  def new(opts) do
    IO.puts("\nBuild #{__MODULE__}, opts: #{inspect(opts)}")
    %@m{base_state: opts[:tab]}
  end

  def put(this, key, value) do
    Map.put(this, :base_state, base_ets(:put, [this.base_state, key, value]))
  end

  def delete(this, key) do
    Map.put(this, :base_state, base_ets(:delete, [this.base_state, key]))
  end

  def fetch(this, key) do
    base_ets(:fetch, [this.base_state, key])
  end

  def keys(this) do
    IO.inspect("this #{inspect(this)}")
    # keys = base_ets(:keys, [this.base_state])
    keys = base_ets(:keys, [this.base_state])
    IO.puts("\n\n\n\n\nkeys #{inspect(keys)}")
    keys
  end

  def nuke(this) do
    # This is silly
    Map.put(this, :base_state, base_ets(:nuke, [this.base_state]))
  end

  def transactional(_), do: {:error, :unsupported}

  defp base_ets(fun, args) do
    IO.puts("calling BaseETS.#{fun}#{inspect(args)}")
    apply(BaseETS, fun, args)
  end
end
