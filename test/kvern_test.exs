defmodule KvernTest do
  use ExUnit.Case, async: false
  doctest Kvern

  @store __MODULE__

  @dir_default File.cwd!() |> Path.join("test/stores/d1-default")
  @dir_poison File.cwd!() |> Path.join("test/stores/d2-poison")

  setup_all do
    Kvern.Repo.Disk.reset_dir(@dir_default)
    Kvern.Repo.Disk.reset_dir(@dir_poison)
    Application.ensure_started(:kvern)
    launch_store()
    :ok
  end

  def launch_store() do
    launch_store(@store, @dir_default)
  end

  def launch_store(store, dir, codec \\ nil) do
    {:ok, _} = Kvern.open(store, disk_copy: dir, codec: codec)
  end

  test "put / get simple value" do
    key = "mykey"
    val = :some_value
    assert :ok === Kvern.put!(@store, key, val)
    assert :error === Kvern.fetch(@store, "__no_exist__")
    assert nil === Kvern.get(@store, "__no_exist__")
    assert :__hey__ === Kvern.get(@store, "__no_exist__", :__hey__)
    recup = Kvern.get(@store, key)
    assert is_pid(Kvern.whereis(@store))

    assert recup === val
  end

  test "get lazy" do
    key = "my_lazy_key"
    # the key does not exist yet, but this is a test that we can delete
    # unexisting keys without error.
    assert :ok === Kvern.delete(@store, key)
    assert :error === Kvern.fetch(@store, key)
    assert :generated === Kvern.get_lazy(@store, key, fn -> :generated end)
    # Unfortunately, values are not automatically set on the store with get_lazy
    assert :error === Kvern.fetch(@store, key)
  end

  test "keys and delete" do
    keys_before = Kvern.keys(@store)
    assert is_list(keys_before)
    assert(not ("k1" in keys_before))
    assert(not ("k2" in keys_before))
    assert(not ("k3" in keys_before))
    assert :ok === Kvern.put(@store, "k1", 1)
    assert :ok === Kvern.put(@store, "k2", 1)
    assert :ok === Kvern.put(@store, "k3", 1)
    keys_full = Kvern.keys(@store)
    assert("k1" in keys_full)
    assert("k2" in keys_full)
    assert("k3" in keys_full)
    assert :ok === Kvern.delete(@store, "k1")
    assert :ok === Kvern.delete(@store, "k2")
    assert :ok === Kvern.delete(@store, "k3")
    keys_end = Kvern.keys(@store)
    assert Enum.sort(keys_end) === Enum.sort(keys_before)
  end

  test "delete / recover" do
    key = "ghost"
    assert :ok === Kvern.put(@store, key, "Tom Joad")
    assert :ok === Kvern.delete(@store, key)
    Kvern.shutdown(@store)
    {:ok, _pid} = launch_store()
    assert :error === Kvern.fetch(@store, key)
  end

  @tag :skip2
  test "simple transaction rollback" do
    key = "tkey"
    val = %{xyz: "This is some value"}
    new_val = "__some_other_value__"
    assert :ok === Kvern.put!(@store, key, val)
    assert Kvern.tainted(@store) === []
    # BEGIN
    assert :ok === Kvern.begin(@store)
    # before put, the store has access to all the data existing before the
    # transaction
    assert val === Kvern.get(@store, key)
    assert :ok === Kvern.put!(@store, key, new_val)
    assert Kvern.tainted(@store) === [key]
    # before rolling back, assert that the new value is readable
    assert new_val === Kvern.get(@store, key)
    # ROLLBACK
    assert :ok === Kvern.rollback(@store)
    assert Kvern.tainted(@store) === []
    assert val === Kvern.get(@store, key)
  end

  @tag :skip2
  test "simple transaction commit" do
    key = "tkey"
    val = %{xyz: "This is some value"}
    new_val = "__some_other_value__"
    assert :ok === Kvern.put!(@store, key, val)
    assert Kvern.tainted(@store) === []
    # BEGIN
    assert :ok === Kvern.begin(@store)
    assert :ok === Kvern.put!(@store, key, new_val)
    assert Kvern.tainted(@store) === [key]
    assert new_val === Kvern.get(@store, key)
    # COMMIT
    assert :ok === Kvern.commit(@store)
    assert Kvern.tainted(@store) === []
    assert new_val === Kvern.get(@store, key)
  end

  test "print a dump" do
    :ok = Kvern.nuke(@store)
    :ok = Kvern.put!(@store, "some_int", 1_234_567)
    :ok = Kvern.put!(@store, "some_float", 1.001002003)
    :ok = Kvern.put!(@store, "some_string", "I see rain ... ")
    :ok = Kvern.put!(@store, "some_map", %{:a => 1, %{inc: "eption"} => false})
    :ok = Kvern.put!(@store, "some_list", [:a, :b, :c])
    :ok = Kvern.put!(@store, "aaaaa", ~s(this is a small s string))
    :ok = Kvern.put!(@store, "bbbbb", ~S(big S here))
    :ok = Kvern.put!(@store, "ccccc", ~w(this is a word list))
    Kvern.print_dump(@store)
    Process.sleep(500)
  end

  test "call by pid" do
    [{pid, _}] = Registry.lookup(Kvern.Registry, @store)
    assert is_pid(pid)
    assert :ok === Kvern.put(pid, "ignore", :ignore)
  end

  test "restore" do
    :ok = Kvern.nuke(@store)
    :ok = Kvern.put(@store, "my-key-1", :my_value_1)
    :ok = Kvern.put(@store, "my-key-2", :my_value_2)
    :ok = Kvern.delete(@store, "my-key-2")
    Kvern.shutdown(@store)
    {:ok, _pid} = launch_store()
    assert :my_value_1 === Kvern.fetch!(@store, "my-key-1")
    # fetch twice for warmup
    Kvern.fetch!(@store, "my-key-1")
    assert :error === Kvern.fetch(@store, "my-key-2")
  end

  test "restore json" do
    # Poison cannot encode atoms as values, so here we will try integers,
    # strings, maps, lists ... but all those tests are actually Poison's unit
    # tests

    # @todo Test atoms as keys.

    store = JSON.Store
    codec = [module: Poison, ext: ".json", encode: [pretty: true]]

    launch_json_store = fn ->
      launch_store(store, @dir_poison, codec)
    end

    val_1 = [1, 2, "three", %{"figure" => 'four'}]
    val_2 = %{"a" => "1", "b" => nil, "c" => 1.001}
    val_3 = "I will be deleted"

    launch_json_store.()
    :ok = Kvern.nuke(store)
    :ok = Kvern.put(store, "json-1", val_1)
    :ok = Kvern.put(store, "json-2", val_2)
    :ok = Kvern.put(store, "json-3-del", val_3)
    :ok = Kvern.delete(store, "json-3-del")
    Kvern.shutdown(store)
    {:ok, _pid} = launch_json_store.()
    assert val_1 === Kvern.fetch!(store, "json-1")
    assert val_2 === Kvern.fetch!(store, "json-2")
    assert :error === Kvern.fetch(store, "json-3")
  end
end
