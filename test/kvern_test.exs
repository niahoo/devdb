defmodule KvernTest do
  use ExUnit.Case, async: false
  doctest Kvern

  @store __MODULE__

  @dir_1 File.cwd!() |> Path.join("test/stores/d1")

  @dir_2 File.cwd!() |> Path.join("test/stores/d2")

  setup_all do
    # setup Kvern
    File.rm_rf!(@dir_1)
    File.mkdir_p!(@dir_1)
    File.rm_rf!(@dir_2)
    File.mkdir_p!(@dir_2)
    Application.ensure_started(:kvern)
    launch_store()
    :ok
  end

  def launch_store() do
    {:ok, _} = Kvern.open(@store)
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
    assert :ok === Kvern.delete(@store, key)
    assert :error === Kvern.fetch(@store, key)
    assert :generated = Kvern.get_lazy(@store, key, fn -> :generated end)
    assert {:ok, :generated} === Kvern.fetch(@store, key)
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
    assert :error = Kvern.fetch(@store, key)
  end

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

  test "call by pid" do
    [{pid, _}] = Registry.lookup(Kvern.Registry, @store)
    assert is_pid(pid)
    assert :ok === Kvern.put(pid, "ignore", :ignore)
  end

  test "restore" do
    :ok = Kvern.nuke(@store)
    :ok = Kvern.put(@store, "my-key-1", :my_value_1)
    :ok = Kvern.put(@store, "my-key-2", :my_value_2)
    Kvern.shutdown(@store)
    {:ok, _pid} = launch_store()
    assert :my_value_1 = Kvern.fetch!(@store, "my-key-1")
  end
end
