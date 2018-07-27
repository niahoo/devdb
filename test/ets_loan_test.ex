defmodule EtsBrokerTest do
  use ExUnit.Case, async: true

  test "start/stop the table broker, borrow the table, get a result, from the same process" do
    {:ok, pid} = EtsBroker.start_link()
    assert is_pid(pid)

    result =
      EtsBroker.borrow(pid, fn tab, _ ->
        assert true === :ets.insert(tab, {:some, :test})
        {:my_pid, self()}
      end)

    assert {:my_pid, pid_from_borrow} = result
    assert pid_from_borrow === self()
    EtsBroker.stop(pid)
  end

  test "get the initial value" do
    {:ok, pid} = EtsBroker.start_link(meta: :something)

    EtsBroker.borrow(pid, fn tab, meta ->
      assert :something === meta
    end)

    EtsBroker.stop(pid)
  end

  test "generate the initial value with a fun" do
    {:ok, pid} =
      EtsBroker.start_link(
        meta: fn ->
          :from_inside_a_fun
        end
      )

    EtsBroker.borrow(pid, fn tab, meta ->
      assert :from_inside_a_fun === meta
    end)

    EtsBroker.stop(pid)
  end

  test "acquiring when already acquired" do
    {:ok, pid} = EtsBroker.start_link(meta: :hello)

    EtsBroker.borrow(pid, fn tab, :hello ->
      assert_raise EtsBroker.Error, fn ->
        EtsBroker.borrow(pid, fn tab, :hello ->
          nil
        end)
      end
    end)
  end

  test "sharing between processes, seeding and crashing" do
    # A process will acquire the table and crash, so the table will be
    # destroyed. Then the table will be restarted, so the initial data set in
    # seed will be retrieved. We need a supervisor for this test for the table
    # holding process to be restarted.

    sl = NamedEtsBroker
    exit_reason = :crashed_on_purpose

    seeder = fn tab ->
      true = :ets.insert(tab, {:control, :initial})
    end

    sup_children = [{EtsBroker, [name: sl, meta: :supervised_table_crash_test, seed: seeder]}]
    sup_opts = [strategy: :one_for_one]

    {:ok, sup} = Supervisor.start_link(sup_children, sup_opts)
    assert is_pid(Process.whereis(sl))

    # Check the seed from the parent process

    EtsBroker.borrow(sl, fn tab ->
      assert [{:control, :initial}] === :ets.lookup(tab, :control)
    end)

    {:ok, sidekick} =
      Sidekick.spawn(fn sidekick ->
        # First we wait for the parent to set an initial value
        Sidekick.join(sidekick, :parent_started_table)
        # Now we borrow the table and check if we can access the value. Then we
        # update it.
        EtsBroker.borrow(sl, fn tab ->
          assert [{:control, :initial}] === :ets.lookup(tab, :control)
          true = :ets.insert(tab, {:control, :updated})
          # We will join our parent process before releasing the table. The
          # broker loop should wait for our release before lending the table to
          # the parent
          Sidekick.join(sidekick, :child_set_updated)
          Process.sleep(500)
        end)

        # Wait for the parent to tell us to do the crash test
        Sidekick.join(sidekick, :borrow_and_crash)

        EtsBroker.borrow(sl, fn tab ->
          IO.puts("Child crashing")
          Process.sleep(1000)
          exit(exit_reason)
        end)
      end)

    # Allow the child to update and wait for him to set an updated value
    Sidekick.join(sidekick, :parent_started_table)
    Sidekick.join(sidekick, :child_set_updated)

    EtsBroker.borrow(sl, fn tab ->
      assert [{:control, :updated}] === :ets.lookup(tab, :control)
    end)

    # now we have controlled, we let the child crash while it has control
    mref = Process.monitor(Sidekick.pid_of(sidekick))
    Sidekick.join(sidekick, :borrow_and_crash)

    receive do
      {:DOWN, ^mref, :process, _, ^exit_reason} ->
        IO.puts("Child crashed successfully :)")
        Process.sleep(1000)
    end

    # once the child has crashed, it must have crashed the table, so we will
    # retrieve the initial value in the table.
    # Before, we must wait for the supervisor to restart the table
    Process.sleep(500)

    EtsBroker.borrow(sl, fn tab ->
      assert [{:control, :initial}] === :ets.lookup(tab, :control)
    end)
  end

  test "Multi concurrency update" do
    # We prove that borrowing the table is truly a exclusive access to the table.
    # We start a bunch of processes, each will borrow the table and during the
    # borrow will :
    # - read the value of the counter
    # - sleep a while
    # - increment the counter
    #
    # Despite the sleep (so in a concurrent table, each process read the same
    # value), every process will increment a new value.
    base_value = 100

    {:ok, sl} =
      EtsBroker.start_link(seed: fn tab -> :ets.insert(tab, {:my_counter, base_value}) end)

    child_count = 20

    parent = self()

    1..child_count
    |> Enum.map(fn x ->
      spawn(fn ->
        EtsBroker.borrow(sl, :infinity, fn tab ->
          # :io.format('~p borrow ', [x])
          [{:my_counter, n}] = :ets.lookup(tab, :my_counter)
          # :io.format('sleep ', [])
          Process.sleep(100)
          # :io.format('insert ~p~n', [n + 1])
          :ets.insert(tab, [{:my_counter, n + 1}])
          send(parent, {:done, x})
        end)
      end)
    end)

    IO.puts("Tasks launched")

    1..child_count
    |> Enum.map(fn x ->
      receive do
        {:done, ^x} -> :ok
      end
    end)

    IO.puts("Tasks finished")

    EtsBroker.borrow(sl, :infinity, fn tab ->
      [{:my_counter, n}] = :ets.lookup(tab, :my_counter)
      assert n === base_value + child_count
    end)
  end
end
