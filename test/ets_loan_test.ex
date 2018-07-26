defmodule EtsLoanTest do
  use ExUnit.Case, async: true

  test "start/stop the single lock" do
    {:ok, pid} = EtsLoan.start_link()
    assert is_pid(pid)
    EtsLoan.stop(pid)
  end

  test "stop the single lock while locked" do
    {:ok, pid} = EtsLoan.start_link()
    assert is_pid(pid)
    assert {:ok, _} = EtsLoan.acquire(pid)

    assert_raise EtsLoan.Error, fn ->
      EtsLoan.stop(pid)
    end

    EtsLoan.force_stop(pid)
  end

  test "get the initial value" do
    {:ok, pid} = EtsLoan.start_link(meta: :something)
    assert {:ok, {_, :something}} = EtsLoan.acquire(pid)

    EtsLoan.force_stop(pid)
  end

  test "generate the initial value with a fun" do
    {:ok, pid} =
      EtsLoan.start_link(
        meta: fn ->
          :from_inside_a_fun
        end
      )

    assert {:ok, {_, :from_inside_a_fun}} = EtsLoan.acquire(pid)

    EtsLoan.force_stop(pid)
  end

  test "test acquiring when already acquired" do
    {:ok, pid} = EtsLoan.start_link(meta: :something)
    {:ok, {_, :something}} = EtsLoan.acquire(pid)

    assert_raise EtsLoan.Error, fn ->
      EtsLoan.acquire(pid)
    end
  end

  test "test sharing between processes and crashing" do
    # A process will acquire the table and crash, so the table will be
    # destroyed. Then the table will be restarted, so the initial meta will be
    # retrieved. We need a supervisor for this test for the table holding
    # process to be restarted.

    sl = NamedEtsLoan

    sup_children = [{EtsLoan, [name: sl, meta: :some_starting_data]}]
    sup_opts = [strategy: :one_for_one]

    {:ok, sup} = Supervisor.start_link(sup_children, sup_opts)
    assert is_pid(EtsLoan.whereis(sl))

    {:ok, sidekick} =
      Sidekick.spawn(fn sidekick ->
        IO.puts("Child acquired 1")
        {:ok, {tab, :some_starting_data}} = EtsLoan.acquire(sl)
        EtsLoan.give_away(sl, tab, :some_new_data)
        IO.puts("Child Released 1")
        Sidekick.join(sidekick, :child_has_released)
        Sidekick.join(sidekick, :parent_has_released)
        # parent should not have changed the meta data
        IO.puts("Child acquired 2")
        {:ok, {tab, :some_new_data}} = EtsLoan.acquire(sl)
        Sidekick.join(sidekick, :will_crash)
        IO.puts("Child will crash #{inspect(self())}")
        exit(:crashed_on_test_purpose)
      end)

    Sidekick.join(sidekick, :child_has_released)
    IO.puts("Parent acquires 1 #{inspect(self())}")
    assert {:ok, {tab, :some_new_data}} = EtsLoan.acquire(sl)
    # we do not change the metadata
    EtsLoan.give_away(sl, tab)
    IO.puts("Parent releases 1 #{inspect(self())}")
    Sidekick.join(sidekick, :parent_has_released)
    # Once the child has acquired the table again, it will crash. We sleep to
    # the supervisor can restart the table holder.
    Sidekick.join(sidekick, :will_crash)
    IO.puts("Parent waits for restart")
    Process.sleep(500)
    # Here the child crashed after acquiring the lock. We should be able to
    # acquire the lock but the metadata should be those givent on start !
    assert EtsLoan.whereis(sl) === Process.whereis(sl)
    IO.puts("Parent acquires 2 #{inspect(self())}")
    assert {:ok, {_, :some_starting_data}} = EtsLoan.acquire(sl, 1000)
  end

  test "Multi concurrency update" do
    {:ok, sl} = EtsLoan.start_link(meta: 0)

    maxcount = 100

    tasks =
      1..maxcount
      |> Enum.map(fn x ->
        Task.async(fn ->
          # If each task could access the value concurrently, most of them would
          # read 0 and set 1, so the result would be inferior to maxcount. Here, every
          # acquire/release is serialized.
          {:ok, {tab, n}} = EtsLoan.acquire(sl)
          # Process.sleep(100)
          # IO.puts("puting #{n + 1}")
          :ok = EtsLoan.give_away(sl, tab, n + 1)
        end)
      end)

    IO.puts("Tasks launched")

    Enum.map(tasks, &Task.await/1)

    assert {:ok, {tab, ^maxcount}} = EtsLoan.acquire(sl)
    :ok = EtsLoan.give_away(sl, tab)
  end

  test "Multi concurrency no-release" do
    {:ok, sl} = EtsLoan.start_link()

    {:ok, {tab, _}} = EtsLoan.acquire(sl)
    :ets.insert(tab, {:counter, 0})
    EtsLoan.give_away(sl, tab)

    maxcount = 50

    tasks =
      1..maxcount
      |> Enum.map(fn x ->
        Task.async(fn ->
          {:ok, {tab, _}} = EtsLoan.acquire(sl)
          # If each task could get the table concurrently, they
          # would all get the initial value here
          [{:counter, n}] = :ets.lookup(tab, :counter)
          Process.sleep(100)
          # Obviously we would use ets:update_counter here but this is a test ;)
          :ets.insert(tab, [{:counter, n + 1}])
        end)
      end)

    IO.puts("Tasks launched")

    Enum.map(tasks, &Task.await/1)

    {:ok, {tab, _}} = EtsLoan.acquire(sl)
    assert [{:counter, maxcount}] === :ets.lookup(tab, :counter)
  end
end
