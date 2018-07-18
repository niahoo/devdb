defmodule SingleLock.Error do
  @moduledoc false

  defexception [:message]

  @doc false
  def exception(msg), do: %__MODULE__{message: msg}
end

defmodule SingleLock do
  use GenLoop
  require Logger

  def start(opts \\ []) do
    start(:start, opts)
  end

  def start_link(opts \\ []) do
    start(:start_link, opts)
  end

  defp start(gen_fun, opts) do
    arg =
      Keyword.take(opts, [:value])
      |> Keyword.put_new(:value, %{})

    gen_opts = Keyword.take(opts, [:name])
    apply(GenLoop, gen_fun, [__MODULE__, arg, gen_opts])
  end

  def stop(sl, reason \\ :normal, timeout \\ :infinity) do
    if Process.get({__MODULE__, :locked}) === true do
      raise SingleLock.Error, message: "Cannot stop while lock is acquired"
    end

    GenLoop.stop(sl, reason, timeout)
  end

  @doc false
  def force_stop(sl, reason \\ :normal, timeout \\ :infinity) do
    GenLoop.stop(sl, reason, timeout)
  end

  def acquire(sl, timeout \\ :infinity) do
    if Process.get({__MODULE__, :locked}) === true do
      raise SingleLock.Error, message: "Cannot acquire while lock is already acquired"
    end

    Process.put({__MODULE__, :locked}, true)
    GenLoop.call(sl, {:acquire, self()}, timeout)
  end

  def release(sl) do
    Process.put({__MODULE__, :locked}, false)
    GenLoop.cast(sl, {:release, self()})
    :ok
  end

  def release(sl, new_value) do
    Process.put({__MODULE__, :locked}, false)
    GenLoop.cast(sl, {:release, self(), new_value})
    :ok
  end

  ## -- server side --

  defmodule S do
    defstruct value: nil, client: nil
  end

  def init(arg) do
    initial_value =
      case arg[:value] do
        fun when is_function(fun, 0) ->
          fun.()

        fun when is_function(fun) ->
          raise SingleLock.Error, "SingleLock initial value cannot be a fun with a non-zero arity"

        term ->
          term
      end

    state =
      %S{}
      |> set_value(initial_value)

    {:ok, state}
  end

  def enter_loop(state) do
    loop_await_client(state)
  end

  def loop_await_client(%{client: nil} = state) do
    receive state do
      rcall(from, {:acquire, client_pid}) when is_pid(client_pid) ->
        reply(from, {:ok, state.value})

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

      rcast({:release, ^client_pid, new_value}) ->
        state
        |> cleanup_lock()
        |> set_value(new_value)
        |> loop_await_client()

      {:DOWN, ^mref, :process, ^client_pid, info} ->
        # Logger.debug("Client #{inspect(client_pid)} DOWN, #{inspect(info)}")

        state
        |> cleanup_lock()
        |> loop_await_client()
    end
  end

  defp set_lock(state, client_pid) do
    mref = Process.monitor(client_pid)
    %{state | client: {client_pid, mref}}
  end

  defp cleanup_lock(%{client: {_client_pid, mref}} = state) do
    Process.demonitor(mref, [:flush])
    %{state | client: nil}
  end

  defp set_value(state, value) do
    %{state | value: value}
  end
end
