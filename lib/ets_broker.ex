defmodule EtsBroker.Error do
  @moduledoc false

  defexception [:message]

  @doc false
  def exception(msg), do: %__MODULE__{message: msg}
end

defmodule EtsBroker do
  use GenLoop
  require Logger

  def child_spec(arg) do
    default = %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
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
      Keyword.take(opts, [:meta, :ets_name, :ets_opts, :seed, :create_table])
      |> Keyword.put_new(:meta, nil)
      |> Keyword.put_new(:ets_name, __MODULE__)
      |> Keyword.put_new(:ets_opts, [:set, :private])
      |> Keyword.put_new(:seed, fn _tab -> :ok end)
      |> Keyword.put_new(:create_table, fn arg ->
        :ets.new(arg[:ets_name], arg[:ets_opts])
      end)

    gen_opts = Keyword.take(opts, [:name])
    apply(GenLoop, gen_fun, [__MODULE__, arg, gen_opts])
  end

  def stop(sl, reason \\ :normal, timeout \\ :infinity) do
    GenLoop.stop(sl, reason, timeout)
  end

  def borrow(sl, timeout, fun) when is_function(fun, 1) do
    discard_meta = fn tab, _meta -> fun.(tab) end
    borrow(sl, timeout, discard_meta)
  end

  def borrow(sl, timeout \\ 5000, fun) do
    proc_key = {__MODULE__, {:locked, sl}}

    if Process.get(proc_key) === true do
      raise EtsBroker.Error, message: "Cannot borrow table while already borrowed."
    end

    Process.put(proc_key, true)

    {:ok, ref} = require_table(sl, timeout)
    {:ok, tab, owner, meta} = receive_table(ref)

    result = fun.(tab, meta)
    release_table(tab, owner)
    Process.delete(proc_key)
    result
  end

  defp require_table(sl, timeout) do
    ref = make_ref
    :ok = GenLoop.call(sl, {:acquire, self(), ref}, timeout)
    {:ok, ref}
  end

  defp receive_table(ref) do
    receive do
      {:"ETS-TRANSFER", tab, from_pid, {^ref, meta}} ->
        {:ok, tab, from_pid, meta}
    after
      0 ->
        raise EtsBroker.Error, message: "An ets give_away message must have been received here"
    end
  end

  defp release_table(tab, to_pid) do
    :ets.give_away(tab, to_pid, :release)
  end

  ## -- server side --

  defmodule S do
    defstruct tab: nil, meta: nil, client: nil
  end

  def init(arg) do
    initial_meta = generate_initial_metadata(arg[:meta])

    tab = arg[:create_table].(arg)
    :ets.setopts(tab, [{:heir, self(), :"HEIR-TRANSFER"}])

    case arg[:seed] do
      seed_fun when is_function(seed_fun, 1) ->
        seed_fun.(tab)

      seed_fun when is_function(seed_fun) ->
        raise EtsBroker.Error, "EtsBroker initial value cannot be a seed_fun with a non-1 arity"

      _ ->
        :ok
    end

    state = %S{tab: tab, meta: initial_meta}

    # Logger.debug("EtsBroker starting with meta = #{inspect(initial_meta)}")
    {:ok, state}
  end

  defp generate_initial_metadata(fun) when is_function(fun, 0), do: fun.()

  defp generate_initial_metadata(fun) when is_function(fun),
    do: raise(EtsBroker.Error, "EtsBroker initial value cannot be a fun with a non-zero arity")

  defp generate_initial_metadata(term), do: term

  def enter_loop(state) do
    loop_await_client(state)
  end

  def loop_await_client(%{client: nil} = state) do
    receive state do
      rcall(from, {:acquire, client_pid, ref}) ->
        # We will give_away the table before replying, so when the client
        # receives the reply, the ets give_away msg is guaranteed to be in its
        # mailbox.
        :ets.give_away(state.tab, client_pid, {ref, state.meta})
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
      {:"ETS-TRANSFER", tab, from, :"HEIR-TRANSFER"} ->
        handle_client_terminated(state, from)

      {:"ETS-TRANSFER", tab, _from, :release} ->
        state
        |> cleanup_lock()
        |> loop_await_client()
    end
  end

  defp handle_client_terminated(%S{client: {client_pid, mref}} = state, client_pid) do
    receive do
      {:DOWN, ^mref, :process, ^client_pid, reason} when reason in [:shutdown, :normal] ->
        # The client exited gracefully so we continue to keep the ets table. We
        # do not change the registered meta.
        Logger.warn(
          "Client exited gracefully. Client should let the borrow function return. KEEPING TABLE DATA."
        )

        state
        |> cleanup_lock()
        |> loop_await_client()

      {:DOWN, ^mref, :process, ^client_pid, reason} ->
        # The client crashed, it could have left the table in an unstable state,
        # so we must also crash.
        Logger.error("Client exited: #{inspect(client_pid)} #{inspect(reason)}")
        exit({:client_fail, reason})
    after
      1000 ->
        raise EtsBroker.Error,
          message: "Received a heir transfer but no 'DOWN' message from monotored client."
    end
  end

  defp handle_client_terminated(%S{client: {client_pid, mref}} = state, other_ets_owner) do
    # @todo provide a give_away function that allow this module to set a new client_pid and monitor
    raise EtsBroker.Error, "Expected ets owner does not match the registered client"
  end

  defp set_lock(state, client_pid) do
    mref = Process.monitor(client_pid)
    %{state | client: {client_pid, mref}}
  end

  defp cleanup_lock(%{client: {_client_pid, mref}} = state) do
    Process.demonitor(mref, [:flush])
    %{state | client: nil}
  end
end
