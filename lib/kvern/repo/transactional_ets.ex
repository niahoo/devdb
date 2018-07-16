defmodule Kvern.Repo.TransactionalETS do
  use TODO
  @m __MODULE__
  @behaviour Kvern.Repo
  alias Kvern.Repo.Ets, as: BaseETS

  @todo """
  Optimize by not reassigning base_state as it's just an ETS reference and never
  changes ?
  """

  defstruct base_state: nil, backup: %{}

  def new(opts) do
    IO.puts("\nBuild #{__MODULE__}, opts: #{inspect(opts)}")
    %@m{base_state: opts[:tab]}
  end

  def put(this, key, value) do
    IO.puts("Inside put !")
    # We cannot use pipes here as order of operations will call the value for
    # Map.put (i.e. the actual ETS modification) befor the Map subject (i.e.
    # this |> backup_val(key))
    this = backup_val(this, key)
    Map.put(this, :base_state, base_ets(:put, [this.base_state, key, value]))
  end

  def delete(this, key) do
    this = backup_val(this, key)
    Map.put(this, :base_state, base_ets(:delete, [this.base_state, key]))
  end

  defp backup_val(this, key) do
    # We want to backup values only once, at first modification. After this
    # point, if any modification of the same key occurs, it changes the value
    # set within the transaction, which we do not care.
    new_backup =
      Map.put_new_lazy(this.backup, key, fn ->
        IO.puts("BACKUP #{key}")

        val =
          case base_ets(:fetch, [this.base_state, key]) do
            {:ok, original_value} ->
              # If the key existed and we changed or deleted it, we will be able to put it as
              # it was
              {:updated, original_value}

            :error ->
              # If the key did not exist, we will know it was an insert (or we
              # deleted unexistant stuff) so we will be able to delete it
              :inserted
          end

        IO.puts(" =  #{inspect(val)}")
        val
      end)

    IO.puts("new backup : #{inspect(new_backup)}")
    Map.put(this, :backup, new_backup)
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

  def rollback(this) do
    new_base_state =
      this.backup
      |> Enum.reduce(this.base_state, &revert_change/2)

    %@m{this | base_state: new_base_state}
  end

  def commit(this) do
    # When committing, there is nothing to do with the data as the table is
    # already up to date.
    # We will only clean the backup
    new_this = Map.put(this, :backup, %{})
    # The ETS repo expets an update to perfom on the table to validate the
    # transaction. Let it know that all is good with an empty list :)
    ets_updates = []
    {:ok, new_this, ets_updates}
  end

  defp revert_change({key, {:updated, original_value}}, base_state) do
    base_ets(:put, [base_state, key, original_value])
  end

  defp revert_change({key, :inserted}, base_state) do
    base_ets(:delete, [base_state, key])
  end

  defp base_ets(fun, args) do
    # IO.puts("calling BaseETS.#{fun}#{inspect(args)}")
    apply(BaseETS, fun, args)
  end
end
