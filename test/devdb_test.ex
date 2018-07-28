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

  test "transaction/commit" do
    pid = start_db!(@db1)
    deleted_key = "delme"
    deleted_val = "osef"
    inserted_key = "addme"
    inserted_val = "some val inserted"
    updated_key = "changeme"
    original_val = "some val already here"
    updated_val = "some val changed"

    DevDB.put(pid, updated_key, original_val)
    DevDB.put(pid, deleted_key, deleted_val)
    DevDB.put(pid, "dummy-1", "dummy val 1")
    DevDB.put(pid, "dummy-2", "dummy val 2")
    DevDB.put(pid, "dummy-3", "dummy val 3")
    DevDB.put(pid, "dummy-4", "dummy val 4")
    DevDB.put(pid, "dummy-5", "dummy val 5")

    retval =
      DevDB.transaction(pid, fn tr_repo ->
        # Deleting a value existing before the transaction
        assert {:ok, deleted_val} === DevDB.fetch(tr_repo, deleted_key)
        assert :ok = DevDB.delete(tr_repo, deleted_key)

        # Updating a value existing before the transaction.
        todo("This assert belongs to the rollback test")
        assert {:ok, original_val} === DevDB.fetch(tr_repo, updated_key)
        assert :ok === DevDB.put(tr_repo, updated_key, updated_val)
        assert {:ok, updated_val} === DevDB.fetch(tr_repo, updated_key)

        # Inserting new values

        # Calling functions on the pid/atom while in transaction should
        # fail as we MUST use the given repository
        assert_raise(DevDB.Error, fn ->
          DevDB.put(pid, inserted_key, inserted_val)
        end)

        assert :error = DevDB.fetch(tr_repo, inserted_key)

        assert :ok === DevDB.put(tr_repo, inserted_key, inserted_val)
        assert {:ok, inserted_val} === DevDB.fetch(tr_repo, inserted_key)

        Process.sleep(100)

        {:ok, :some_return}
      end)

    assert :error === DevDB.fetch(pid, deleted_key)
    assert {:ok, updated_val} === DevDB.fetch(pid, updated_key)
    assert {:ok, inserted_val} === DevDB.fetch(pid, inserted_key)

    assert {:ok, :some_return} = retval
  end

  test "transaction/rollback" do
    pid = start_db!(@db1)
    deleted_key = "delme"
    deleted_val = "osef"
    inserted_key = "addme"
    inserted_val = "some val inserted"
    updated_key = "changeme"
    original_val = "some val already here"
    updated_val = "some val changed"

    DevDB.put(pid, updated_key, original_val)
    DevDB.put(pid, deleted_key, deleted_val)
    DevDB.put(pid, "dummy-1", "dummy val 1")
    DevDB.put(pid, "dummy-2", "dummy val 2")
    DevDB.put(pid, "dummy-3", "dummy val 3")
    DevDB.put(pid, "dummy-4", "dummy val 4")
    DevDB.put(pid, "dummy-5", "dummy val 5")

    retval =
      DevDB.transaction(pid, fn tr_repo ->
        # Deleting a value existing before the transaction
        assert {:ok, deleted_val} === DevDB.fetch(tr_repo, deleted_key)
        assert :ok === DevDB.delete(tr_repo, deleted_key)

        # Updating a value existing before the transaction.
        assert {:ok, original_val} === DevDB.fetch(tr_repo, updated_key)
        assert :ok === DevDB.put(tr_repo, updated_key, updated_val)
        assert {:ok, updated_val} === DevDB.fetch(tr_repo, updated_key)

        # Inserting new values

        # Calling functions on the pid/atom while in transaction should
        # fail as we MUST use the given repository
        assert_raise(DevDB.Error, fn ->
          DevDB.put(pid, inserted_key, inserted_val)
        end)

        assert :error === DevDB.fetch(tr_repo, inserted_key)

        assert :ok === DevDB.put(tr_repo, inserted_key, inserted_val)
        assert {:ok, inserted_val} === DevDB.fetch(tr_repo, inserted_key)

        Process.sleep(100)

        {:error, :rolled_back_custom_error_return}
      end)

    assert {:ok, deleted_val} === DevDB.fetch(pid, deleted_key)
    assert {:ok, original_val} === DevDB.fetch(pid, updated_key)
    assert :error === DevDB.fetch(pid, inserted_key)

    assert {:error, :rolled_back_custom_error_return} === retval
  end

  test "transaction/rollback-double-update" do
    # During the transaction, we update a same value twice. After the rollback,
    # the original value should be there, and not the value before the last put
    # set at put #n -1
    pid = start_db!(@db1)
    updated_key = "changeme"
    original_val = "some val already here"
    updated_val = "some val changed"

    DevDB.put(pid, updated_key, original_val)
    DevDB.put(pid, "dummy-1", "dummy val 1")

    retval =
      DevDB.transaction(pid, fn tr_repo ->
        assert {:ok, original_val} === DevDB.fetch(tr_repo, updated_key)
        assert :ok === DevDB.put(tr_repo, updated_key, "THIS_SHOULD_BE_FORGOTTEN")
        assert :ok === DevDB.put(tr_repo, updated_key, updated_val)
        assert {:ok, updated_val} === DevDB.fetch(tr_repo, updated_key)

        Process.sleep(100)

        {:error, :rolled_back_custom_error_return_2}
      end)

    assert {:ok, original_val} === DevDB.fetch(pid, updated_key)

    assert {:error, :rolled_back_custom_error_return_2} === retval
  end

  defp assert_select(store, selector, expected_keys) do
    expected_keys = Enum.sort(expected_keys)
    result = DevDB.select(store, selector)
    {:ok, select_result} = result

    found_keys =
      select_result
      |> Keyword.keys()
      |> Enum.sort()

    assert expected_keys === found_keys
  end

  test "simple select" do
    pid = start_db!(@db1)

    DevDB.put(pid, "key-1", 100)
    DevDB.put(pid, "key-2", 100)
    DevDB.put(pid, "key-3", 200)
    DevDB.put(pid, "key-4", 200)
    DevDB.put(pid, "key-5", 500)
    assert_select(pid, fn val, _key -> val > 100 end, ["key-3", "key-4", "key-5"])
  end

  test "select in transaction" do
    pid = start_db!(@db1)

    DevDB.put(pid, "key-1", 100)
    DevDB.put(pid, "key-2", 100)
    DevDB.put(pid, "key-3", 200)
    DevDB.put(pid, "key-4", 200)
    DevDB.put(pid, "key-5", 500)

    DevDB.transaction(pid, fn tr_repo ->
      assert_select(tr_repo, fn val, _key -> val > 100 end, ["key-3", "key-4", "key-5"])
      DevDB.put(tr_repo, "key-5", 0)
      assert_select(tr_repo, fn val, _key -> val > 100 end, ["key-3", "key-4"])
      :rollback
    end)

    assert_select(pid, fn val, _key -> val > 100 end, ["key-3", "key-4", "key-5"])

    DevDB.transaction(pid, fn tr_repo ->
      assert_select(tr_repo, fn val, _key -> val > 100 end, ["key-3", "key-4", "key-5"])
      DevDB.put(tr_repo, "key-4", 0)
      DevDB.put(tr_repo, "key-6", 600)
      assert_select(tr_repo, fn val, _key -> val > 100 end, ["key-3", "key-5", "key-6"])
      :commit
    end)

    assert_select(pid, fn val, _key -> val > 100 end, ["key-3", "key-5", "key-6"])
  end
end
