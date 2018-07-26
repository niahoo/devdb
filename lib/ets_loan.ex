defmodule EtsLoan.Error do
  @moduledoc false

  defexception [:message]

  @doc false
  def exception(msg), do: %__MODULE__{message: msg}
end

defmodule EtsLoan do
  use GenLoop
  require Logger

  def child_spec(arg) do
    default = %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]},
      restart: :transient
    }
  end

  def start(opts \\ []) do
    start(:start, opts)
  end

  def start_link(opts \\ []) do
    start(:start_link, opts)
  end

  defp start(gen_fun, opts) do
    arg =
      Keyword.take(opts, [:meta, :ets_name, :ets_opts])
      |> Keyword.put_new(:meta, %{_default_meta: true})
      |> Keyword.put_new(:ets_name, __MODULE__)
      |> Keyword.put_new(:ets_opts, [])

    gen_opts = Keyword.take(opts, [:name])
    apply(GenLoop, gen_fun, [__MODULE__, arg, gen_opts])
  end

  def stop(sl, reason \\ :normal, timeout \\ :infinity) do
    if Process.get({__MODULE__, :locked}) === true do
      raise EtsLoan.Error, message: "Cannot stop while lock is acquired"
    end

    GenLoop.stop(sl, reason, timeout)
  end

  @doc false
  def force_stop(sl, reason \\ :normal, timeout \\ :infinity) do
    GenLoop.stop(sl, reason, timeout)
  end

  def acquire(sl, timeout \\ :infinity) do
    if Process.get({__MODULE__, :locked}) === true do
      raise EtsLoan.Error, message: "Cannot acquire while lock is already acquired"
    end

    Process.put({__MODULE__, :locked}, true)
    :ok = GenLoop.call(sl, {:acquire, self()}, timeout)

    receive do
      {:"ETS-TRANSFER", tab, _from, meta} ->
        {:ok, {tab, meta}}
    after
      0 ->
        raise EtsLoan.Error, message: "An ets give_away message must have been received here"
    end
  end

  def give_away(sl, tab, new_meta \\ {__MODULE__, :__KEEP__}) do
    :ets.give_away(tab, __MODULE__.whereis(sl), new_meta)
    Process.put({__MODULE__, :locked}, false)
    :ok
  end

  def whereis(pid) when is_pid(pid), do: pid
  def whereis(atom) when is_atom(atom), do: Process.whereis(atom)

  def loan!(sl, fun) when is_function(fun, 2) do
    {:ok, {tab, meta}} = acquire(sl)
    result = fun.(tab, meta)
    give_away(sl, tab)
    result
  end

  ## -- server side --

  defmodule S do
    defstruct tab: nil, meta: nil, client: nil
  end

  def init(arg) do
    initial_meta =
      case arg[:meta] do
        fun when is_function(fun, 0) ->
          fun.()

        fun when is_function(fun) ->
          raise EtsLoan.Error, "EtsLoan initial value cannot be a fun with a non-zero arity"

        term ->
          term
      end

    tab = :ets.new(arg[:ets_name], arg[:ets_opts])

    :ets.setopts(tab, [{:heir, self(), :"HEIR-TRANSFER"}])

    state =
      %S{tab: tab}
      |> set_meta(initial_meta)

    Logger.debug("EtsLoan starting with meta = #{inspect(initial_meta)}")
    {:ok, state}
  end

  def enter_loop(state) do
    loop_await_client(state)
  end

  def loop_await_client(%{client: nil} = state) do
    receive state do
      rcall(from, {:acquire, client_pid}) when is_pid(client_pid) ->
        # We will give_away the table before replying, so when the client
        # receives the reply, the ets give_away msg is guaranteed to be in its
        # mailbox.
        :ets.give_away(state.tab, client_pid, state.meta)
        reply(from, :ok)

        state
        |> set_lock(client_pid)
        |> loop_await_release()

      info ->
        # Logger.debug("Unhandled info : #{inspect(info)}")
        loop_await_client(state)
    end
  end

  def loop_await_release(%S{client: {client_pid, mref}} = state) when is_pid(client_pid) do
    receive state do
      rcast({:release, ^client_pid}) ->
        state
        |> cleanup_lock()
        |> loop_await_client()

      {:"ETS-TRANSFER", tab, from, :"HEIR-TRANSFER"} ->
        handle_client_terminated(state, from)

      {:"ETS-TRANSFER", tab, _from, new_meta} ->
        state
        |> cleanup_lock()
        |> set_meta(new_meta)
        |> loop_await_client()
    end
  end

  defp handle_client_terminated(%S{client: {client_pid, mref}} = state, client_pid) do
    receive do
      {:DOWN, ^mref, :process, ^client_pid, reason} when reason in [:shutdown, :normal] ->
        # The client exited gracefully so we continue to keep the ets table. We
        # do not change the registered meta.
        state
        |> cleanup_lock()
        |> loop_await_client()

      {:DOWN, ^mref, :process, ^client_pid, reason} ->
        # The client crashed, it could have left the table in an unstable state,
        # so we must also crash.
        Logger.error("Client exited: #{inspect(client_pid)} #{inspect(reason)}")
        exit(reason)
    after
      1000 ->
        raise EtsLoan.Error,
          message: "Received a heir transfer but no 'DOWN' message from monotored client."
    end
  end

  defp handle_client_terminated(%S{client: {client_pid, mref}} = state, other_ets_owner) do
    # @todo provide a give_away function that allow this module to set a new client_pid and monitor
    raise EtsLoan.Error, "Expected ets owner does not match the registered client"
  end

  defp set_lock(state, client_pid) do
    mref = Process.monitor(client_pid)
    %{state | client: {client_pid, mref}}
  end

  defp cleanup_lock(%{client: {_client_pid, mref}} = state) do
    Process.demonitor(mref, [:flush])
    %{state | client: nil}
  end

  defp set_meta(state, {__MODULE__, :__KEEP__}), do: state
  defp set_meta(state, meta), do: %{state | meta: meta}
end
