defmodule KvernTest do
  use ExUnit.Case, async: false
  require Logger
  doctest Kvern

  @dir_default File.cwd!() |> Path.join("test/stores/d1-default")
  @dir_poison File.cwd!() |> Path.join("test/stores/d2-poison")

  @s1 __MODULE__
  @s2json KvernTest.JSON

  @stores_conf %{
    @s1 => [disk_copy: @dir_default],
    @s2json => [
      disk_copy: @dir_poison,
      codec: [module: Poison, ext: ".json", encode: [pretty: true]]
    ]
  }

  setup_all do
    Kvern.Repo.Disk.reset_dir(@dir_default)
    Kvern.Repo.Disk.reset_dir(@dir_poison)
    Application.ensure_started(:kvern)
    launch_store(@s1)
    :ok
  end

  def launch_store(store) do
    conf = Map.fetch!(@stores_conf, store)
    {:ok, _} = Kvern.open(store, conf)
  end

  def restart_store(store) do
    Kvern.shutdown(store)
    {:ok, _pid} = launch_store(store)
  end

  @tag :skip
  test "put / get simple value" do
    key = "mykey"
    val = :some_value
    assert :ok === Kvern.put!(@s1, key, val)
    assert :error === Kvern.fetch(@s1, "__no_exist__")
    assert nil === Kvern.get(@s1, "__no_exist__")
    assert :__hey__ === Kvern.get(@s1, "__no_exist__", :__hey__)
    recup = Kvern.get(@s1, key)
    assert is_pid(Kvern.whereis(@s1))

    assert recup === val
  end

  @tag :skip
  test "get lazy" do
    key = "my_lazy_key"
    # the key does not exist yet, but this is a test that we can delete
    # unexisting keys without error.
    assert :ok === Kvern.delete(@s1, key)
    assert :error === Kvern.fetch(@s1, key)
    assert :generated === Kvern.get_lazy(@s1, key, fn -> :generated end)
    # Unfortunately, values are not automatically set on the store with get_lazy
    assert :error === Kvern.fetch(@s1, key)
  end

  @tag :skip
  test "keys and delete" do
    keys_before = Kvern.keys(@s1)
    assert is_list(keys_before)
    assert(not ("k1" in keys_before))
    assert(not ("k2" in keys_before))
    assert(not ("k3" in keys_before))
    assert :ok === Kvern.put(@s1, "k1", 1)
    assert :ok === Kvern.put(@s1, "k2", 1)
    assert :ok === Kvern.put(@s1, "k3", 1)
    keys_full = Kvern.keys(@s1)
    assert("k1" in keys_full)
    assert("k2" in keys_full)
    assert("k3" in keys_full)
    assert :ok === Kvern.delete(@s1, "k1")
    assert :ok === Kvern.delete(@s1, "k2")
    assert :ok === Kvern.delete(@s1, "k3")
    keys_end = Kvern.keys(@s1)
    assert Enum.sort(keys_end) === Enum.sort(keys_before)
  end

  @tag :skip
  test "delete / recover" do
    key = "ghost"
    assert :ok === Kvern.put(@s1, key, "Tom Joad")
    assert :ok === Kvern.delete(@s1, key)
    restart_store(@s1)
    assert :error === Kvern.fetch(@s1, key)
  end

  test "simple transaction rollback" do
    key = "tkey"
    val = %{xyz: "This is some value"}
    new_val = "__some_other_value__"
    assert :ok === Kvern.put!(@s1, key, val)
    assert Kvern.tainted(@s1) === []
    # BEGIN
    assert :ok === Kvern.begin(@s1)
    Kvern.print_dump(@s1)

    # before put, the store has access to all the data existing before the
    # transaction
    assert val === Kvern.get(@s1, key)
    assert :ok === Kvern.put!(@s1, key, new_val)
    assert Kvern.tainted(@s1) === [key]
    # before rolling back, assert that the new value is readable
    assert new_val === Kvern.get(@s1, key)
    # ROLLBACK
    assert :ok === Kvern.rollback(@s1)
    assert Kvern.tainted(@s1) === []
    assert val === Kvern.get(@s1, key)
  end

  test "rollback advanced" do
    # Logger.warn("@todo test begin/delete/rollback , value should not be deleted")
    # Logger.warn("@todo test begin/put/delete/rollback, value should not be changed")
    # Logger.warn("@todo test begin/delete/put/rollback, value should not be changed")
  end

  test "simple transaction commit" do
    key = "tkey"
    val = %{xyz: "This is some value"}
    new_val = "__some_other_value__"
    assert :ok === Kvern.put!(@s1, key, val)
    assert Kvern.tainted(@s1) === []
    # BEGIN
    assert :ok === Kvern.begin(@s1)
    assert :ok === Kvern.put!(@s1, key, new_val)
    assert Kvern.tainted(@s1) === [key]
    assert new_val === Kvern.get(@s1, key)
    # COMMIT
    assert :ok === Kvern.commit(@s1)
    assert Kvern.tainted(@s1) === []
    assert new_val === Kvern.get(@s1, key)
  end

  test "transaction commit fun and persistence" do
    key = "cpersist"
    old_value = "old-value"
    new_value = "new-value"
    assert :ok === Kvern.put!(@s1, key, old_value)

    assert {:ok, "hello"} ===
             Kvern.transaction(@s1, fn db ->
               Kvern.put!(db, key, new_value)
               {:ok, "hello"}
             end)

    # Now, out of the transaction, the value should be updated, as the arg given
    # to the callback should be the store given to transaction
    assert new_value === Kvern.get(@s1, key)
    restart_store(@s1)
    # Transaction commit updates must have been sent to repo backends
    assert new_value === Kvern.get(@s1, key)
  end

  @tag :skip
  test "print a dump" do
    :ok = Kvern.nuke(@s1)
    :ok = Kvern.put!(@s1, "some_int", 1_234_567)
    :ok = Kvern.put!(@s1, "some_float", 1.001002003)
    :ok = Kvern.put!(@s1, "some_string", "I see rain ... ")
    :ok = Kvern.put!(@s1, "some_map", %{:a => 1, %{inc: "eption"} => false})
    :ok = Kvern.put!(@s1, "some_list", [:a, :b, :c])
    :ok = Kvern.put!(@s1, "aaaaa", ~s(this is a small s string))
    :ok = Kvern.put!(@s1, "bbbbb", ~S(big S here))
    :ok = Kvern.put!(@s1, "ccccc", ~w(this is a word list))
    Kvern.print_dump(@s1)
    Process.sleep(500)
  end

  @tag :skip
  test "call by pid" do
    [{pid, _}] = Registry.lookup(Kvern.Registry, @s1)
    assert is_pid(pid)
    assert :ok === Kvern.put(pid, "ignore", :ignore)
  end

  @tag :skip
  test "restore" do
    :ok = Kvern.nuke(@s1)
    :ok = Kvern.put(@s1, "my-key-1", :my_value_1)
    :ok = Kvern.put(@s1, "my-key-2", :my_value_2)
    :ok = Kvern.delete(@s1, "my-key-2")

    assert :my_value_1 === Kvern.fetch!(@s1, "my-key-1")
    # fetch twice for warmup
    Kvern.fetch!(@s1, "my-key-1")
    assert :error === Kvern.fetch(@s1, "my-key-2")
  end

  @tag :skip
  test "restore json" do
    # Poison cannot encode atoms as values, so here we will try integers,
    # strings, maps, lists ... but all those tests are actually Poison's unit
    # tests

    # @todo Test atoms as keys.

    val_1 = [1, 2, "three", %{"figure" => 'four'}]
    val_2 = %{"a" => "1", "b" => nil, "c" => 1.001}
    val_3 = "I will be deleted"

    :ok = Kvern.nuke(@s2json)
    :ok = Kvern.put(@s2json, "json-1", val_1)
    :ok = Kvern.put(@s2json, "json-2", val_2)
    :ok = Kvern.put(@s2json, "json-3-del", val_3)
    :ok = Kvern.delete(@s2json, "json-3-del")
    restart_store(@s2json)
    assert val_1 === Kvern.fetch!(@s2json, "json-1")
    assert val_2 === Kvern.fetch!(@s2json, "json-2")
    assert :error === Kvern.fetch(@s2json, "json-3")
  end
end
