defmodule SingleLock.Error do
  @moduledoc false

  defexception [:message]

  @doc false
  def exception(msg), do: %__MODULE__{message: msg}
end

defmodule SingleLock do
  use GenLoop

  def start(opts \\ []) do
    opts =
      opts
      |> Keyword.put_new(:value, %{})

    GenLoop.start(__MODULE__, opts)
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
    GenLoop.cast(sl, {:release, self()})
    :ok
  end

  def release(sl, new_value) do
    GenLoop.cast(sl, {:release, self(), new_value})
    :ok
  end

  ## -- server side --

  defmodule S do
    defstruct value: nil, owner: nil
  end

  def init(opts) do
    {:ok, %S{value: opts[:value]}}
  end

  def enter_loop(state) do
    await_client(state)
  end

  def await_client(%{owner: nil} = state) do
    receive state do
      rcall(from, {:acquire, owner_pid}) when is_pid(owner_pid) ->
        reply(from, {:ok, state.value})
        await_release(%S{state | owner: owner_pid})
        rcall(from, :stop)
    end
  end

  def await_release(%S{owner: owner_pid} = state) when is_pid(owner_pid) do
    receive state do
      rcast({:release, ^owner_pid}) ->
        await_client(%S{state | owner: nil})

      rcast({:release, ^owner_pid, new_value}) ->
        await_client(%S{state | owner: nil, value: new_value})
    end
  end
end

defmodule SingleLockTest do
  use ExUnit.Case, async: true

  test "start/stop the single lock" do
    {:ok, pid} = SingleLock.start()
    assert is_pid(pid)
    SingleLock.stop(pid, 1000)
  end

  test "stop the single lock while locked" do
    {:ok, pid} = SingleLock.start()
    assert is_pid(pid)
    assert {:ok, _} = SingleLock.acquire(pid)

    assert_raise SingleLock.Error, fn ->
      SingleLock.stop(pid, 1000)
    end

    SingleLock.force_stop(pid, 1000)
  end

  test "get the initial value" do
    {:ok, pid} = SingleLock.start(name: __MODULE__, value: :something)
    {:ok, :something} = SingleLock.acquire(pid)

    SingleLock.force_stop(pid, 1000)
  end

  test "test aquiring when already acquired" do
    {:ok, pid} = SingleLock.start(name: __MODULE__, value: :something)
    {:ok, :something} = SingleLock.acquire(pid)

    assert_raise SingleLock.Error, fn ->
      SingleLock.acquire(pid)
    end
  end

  test "update the value from another process" do
    {:ok, sl} = SingleLock.start(name: __MODULE__, value: :something)
    new_value = :new_val

    {:ok, sidekick} =
      Sidekick.spawn_link(fn sidekick ->
        {:ok, _} = SingleLock.acquire(sl)
        IO.puts("Child acquired")
        IO.puts("Child released with value = #{inspect(new_value)}")
        :ok = SingleLock.release(sl, new_value)
        Sidekick.join(sidekick, :new_value_is_set)
      end)

    IO.puts("Parent waiting for child to set a new value")
    Sidekick.join(sidekick, :new_value_is_set)
    IO.puts("Child is done, getting value")
    assert {:ok, new_value} === SingleLock.acquire(sl)
  end
end
