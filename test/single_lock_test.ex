defmodule SingleLockTest do
  use ExUnit.Case, async: false

  test "start/stop the single lock" do
    {:ok, pid} = SingleLock.start_link()
    assert is_pid(pid)
    SingleLock.stop(pid)
  end

  test "stop the single lock while locked" do
    {:ok, pid} = SingleLock.start_link()
    assert is_pid(pid)
    assert {:ok, _} = SingleLock.acquire(pid)

    assert_raise SingleLock.Error, fn ->
      SingleLock.stop(pid)
    end

    SingleLock.force_stop(pid)
  end

  test "get the initial value" do
    {:ok, pid} = SingleLock.start_link(value: :something)
    {:ok, :something} = SingleLock.acquire(pid)

    SingleLock.force_stop(pid)
  end

  test "generate the initial value with a fun" do
    {:ok, pid} =
      SingleLock.start_link(
        value: fn ->
          :from_inside_a_fun
        end
      )

    assert {:ok, :from_inside_a_fun} === SingleLock.acquire(pid)

    SingleLock.force_stop(pid)
  end

  test "test acquiring when already acquired" do
    {:ok, pid} = SingleLock.start_link(value: :something)
    {:ok, :something} = SingleLock.acquire(pid)

    assert_raise SingleLock.Error, fn ->
      SingleLock.acquire(pid)
    end
  end

  test "test acquiring but crashing" do
    {:ok, sl} = SingleLock.start_link()

    {:ok, sidekick} =
      Sidekick.spawn(fn sidekick ->
        {:ok, _} = SingleLock.acquire(sl)
        IO.puts("Child acquired")
        Sidekick.join(sidekick, :will_crash)
        IO.puts("Child crash")
        exit(:crashed_on_test_purpose)
      end)

    Sidekick.join(sidekick, :will_crash)

    # Here the child crashed after acquiring the lock. We should be able to
    # acquire the lock !
    assert {:ok, _} = SingleLock.acquire(sl, 1000)
  end

  test "update the value from another process" do
    {:ok, sl} = SingleLock.start_link(value: :something)
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

  test "Multi concurrency update" do
    {:ok, sl} = SingleLock.start_link(value: 0)

    maxcount = 100

    tasks =
      1..maxcount
      |> Enum.map(fn x ->
        Task.async(fn ->
          # If each task could access the value concurrently, most of them would
          # read 0 and set 1, so the result would be inferior to maxcount. Here, every
          # acquire/release is serialized.
          {:ok, n} = SingleLock.acquire(sl)
          # Process.sleep(100)
          :ok = SingleLock.release(sl, n + 1)
        end)
      end)

    IO.puts("Tasks launched")

    Enum.map(tasks, &Task.await/1)

    assert {:ok, maxcount} === SingleLock.acquire(sl)
    :ok = SingleLock.release(sl)
  end

  test "Multi concurrency no-release" do
    # We let the procs shutdown in order to release, and we use an agent to
    # control that every process is executed in a serialized way
    {:ok, sl} = SingleLock.start_link()
    {:ok, agent} = Agent.start_link(fn -> 0 end)

    maxcount = 50

    tasks =
      1..maxcount
      |> Enum.map(fn x ->
        Task.async(fn ->
          # If each task could access the value of the agent concurrently, they
          # would all get the initial value. But because we acquire a Lock
          # before, all is serialized.
          {:ok, _} = SingleLock.acquire(sl)
          n = Agent.get(agent, fn n -> n end)
          Process.sleep(100)
          # Note that we do not use the current state of the agent in the update
          # function, because it MUST be what we got in get ! We could also pin
          # the variable.
          n = Agent.update(agent, fn _ -> n + 1 end)
        end)
      end)

    IO.puts("Tasks launched")

    Enum.map(tasks, &Task.await/1)

    assert maxcount === Agent.get(agent, fn n -> n end)
  end
end
