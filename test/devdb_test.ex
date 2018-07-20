defmodule DevDBTest do
  use ExUnit.Case, async: true
  use TODO, print: :all
  @db1 __MODULE__

  @dbtores_conf %{
    @db1 => []
  }

  defp start_db!(db) do
    conf = Map.get(@dbtores_conf, db)
    {:ok, pid} = DevDB.start_link(db, conf)
    true = is_pid(pid)
    pid
  end

  test "start/stop the database" do
    pid = start_db!(@db1)
    assert is_pid(pid)
    DevDB.stop(pid, :normal, 1000)
  end

  test "put/fetch a value" do
    key = "putfetch"
    pid = start_db!(@db1)
    assert :ok = DevDB.put(pid, key, 1234)
    assert {:ok, 1234} = DevDB.fetch(pid, key)
  end

  test "put/delete/fetch a value" do
    key = "put-del-fetch"
    pid = start_db!(@db1)
    assert :ok = DevDB.put(pid, key, "hello")
    assert :ok = DevDB.delete(pid, key)
    assert :error = DevDB.fetch(pid, key)
  end

  test "transaction/put/fetch/commit/fetch" do
    pid = start_db!(@db1)
    deleted_key = "delme"
    deleted_val = "osef"
    inserted_key = "addme"
    inserted_val = "some val inserted"
    updated_key = "changeme"
    updated_val = "some val already here"
    new_updated_val = "some val changed"

    DevDB.put(pid, updated_key, updated_val)
    DevDB.put(pid, deleted_key, deleted_val)
    DevDB.put(pid, "dummy-1", "dummy val 1")
    DevDB.put(pid, "dummy-2", "dummy val 2")
    DevDB.put(pid, "dummy-3", "dummy val 3")
    DevDB.put(pid, "dummy-4", "dummy val 4")
    DevDB.put(pid, "dummy-5", "dummy val 5")

    retval =
      DevDB.transaction(pid, fn tr_repo ->
        IO.puts("in transaction")

        # Deleting a value existing before the transaction
        assert {:ok, deleted_val} = DevDB.fetch(tr_repo, deleted_key)
        assert :ok = DevDB.delete(tr_repo, deleted_key)

        # Updating a value existing before the transaction. We put two different
        # values inside, so in a rollback we must retrieve the original value,
        # not the previous one set in transaction.
        todo("This assert belongs to the rollback test")
        assert {:ok, updated_val} = DevDB.fetch(tr_repo, updated_key)
        assert :ok = DevDB.put(tr_repo, updated_key, "THIS_SHOULD_BE_FORGOTTEN")
        assert :ok = DevDB.put(tr_repo, updated_key, new_updated_val)
        assert {:ok, new_updated_val} = DevDB.fetch(tr_repo, updated_key)

        # Inserting new values

        # Calling functions on the pid/atom while in transaction should
        # fail as we MUST use the given repository
        assert_raise(DevDB.Error, fn ->
          DevDB.put(pid, inserted_key, inserted_val)
        end)

        assert :error = DevDB.fetch(tr_repo, inserted_key)

        IO.puts(
          "DevDB.put(#{inspect(tr_repo)}, #{inspect(inserted_key)}, #{inspect(inserted_val)})"
        )

        assert :ok = DevDB.put(tr_repo, inserted_key, inserted_val)
        assert {:ok, inserted_val} === DevDB.fetch(tr_repo, inserted_key)

        tr_repo
        |> elem(1)
        |> Map.get(:tab)
        |> :ets.tab2list()
        |> IO.inspect(pretty: true)

        Process.sleep(100)

        :observer.start()
        Process.sleep(:infinity)
        {:ok, :some_return}
      end)

    assert :error === DevDB.fetch(pid, deleted_key)
    assert {:ok, new_updated_val} === DevDB.fetch(pid, updated_key)
    assert {:ok, inserted_val} === DevDB.fetch(pid, inserted_key)

    assert {:ok, :some_return} = retval
  end

  @todo ~S(test "put/delete/shutdown/fetch" do)
  @todo ~S(test "transaction/put/fetch/rollback/fetch" do)
  @todo ~S(test "put/transaction/delete/rollback/fetch" do)
end
