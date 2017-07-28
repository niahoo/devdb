defmodule KvernTest do
  use ExUnit.Case, async: false
  doctest Kvern

  @store __MODULE__

  @dir_1 (
    File.cwd! |> Path.join("test/stores/d1")
  )

  @dir_2 (
    File.cwd! |> Path.join("test/stores/d2")
  )

  setup_all do
    # setup Kvern
    File.rm_rf! @dir_1
    File.mkdir_p! @dir_1
    File.rm_rf! @dir_2
    File.mkdir_p! @dir_2
    Application.ensure_started(:kvern)
    launch_store()
    :ok
  end

  def launch_store() do
    {:ok, _} = Kvern.open(
      name: @store,
      dir: @dir_1,
      codec: Kvern.Codec.Exs
    )
  end

  test "key format" do
    assert :ok = Kvern.check_valid_key("a")
    assert :ok = Kvern.check_valid_key("aqz009_-___zza")
    assert :ok = Kvern.check_valid_key("A")

    assert {:error, _} = Kvern.check_valid_key("0ab")
    assert {:error, _} = Kvern.check_valid_key("1")
    assert {:error, _} = Kvern.check_valid_key("é")
    assert {:error, _} = Kvern.check_valid_key("hàça")

    assert :ok = Kvern.check_valid_key(:binary.copy("a", Kvern.key_maxlen()))
    assert {:error, _} = Kvern.check_valid_key(:binary.copy("a", 1 + Kvern.key_maxlen()))
  end

  test "put / get simple value" do
    key = "mykey"
    val = :some_value
    assert :ok === Kvern.put!(@store, key, val)
    assert :error === Kvern.fetch(@store, "__no_exist__")
    assert nil === Kvern.get(@store, "__no_exist__")
    assert :__hey__ === Kvern.get(@store, "__no_exist__", :__hey__)
    recup = Kvern.get(@store, key)
    assert recup === val
  end

  test "get lazy" do
    key = "my_lazy_key"
    assert :ok === Kvern.delete(@store, key)
    assert :error === Kvern.fetch(@store, key)
    assert :generated = Kvern.get_lazy(@store, key, fn -> :generated end)
  end

  test "keys and delete" do
    keys_before = Kvern.keys(@store)
    assert(not "k1" in keys_before)
    assert(not "k2" in keys_before)
    assert(not "k3" in keys_before)
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
    assert (Enum.sort(keys_end) === Enum.sort(keys_before))
  end

  test "delete / recover" do
    key = "ghost"
    assert :ok === Kvern.put(@store, key, "Tom Joad")
    assert :ok === Kvern.delete(@store, key)
    Kvern.shutdown(@store)
    {:ok, _pid} = launch_store()
    assert :error = Kvern.fetch(@store, key)
  end

  test "simple transaction" do
    key = "tkey"
    val = %{xyz: "This is some value"}
    assert :ok === Kvern.put!(@store, key, val)
    assert :ok === Kvern.begin(@store)
    assert :ok === Kvern.put!(@store, key, "__some_other_value__")
    assert "__some_other_value__" === Kvern.get(@store, key)
    assert :ok === Kvern.rollback(@store)
    assert val === Kvern.get(@store, key)
  end

  test "call by pid" do
    [{pid, _}] = Registry.lookup(Kvern.Registry, @store)
    assert is_pid(pid)
    assert :ok === Kvern.put(pid, "ignore", :ignore)
  end

  test "restore" do
    :ok = Kvern.nuke_storage(@store)
    :ok = Kvern.put(@store, "my-key-1", :my_value_1)
    :ok = Kvern.put(@store, "my-key-2", :my_value_2)
    Kvern.shutdown(@store)
    {:ok, _pid} = launch_store()
    assert :my_value_1 = Kvern.fetch!(@store, "my-key-1")
  end

  test "EDN file format" do
    IO.puts "@todo"
    # launch = fn ->
    #   {:ok, pid} = Kvern.open(
    #     dir: @dir_2,
    #     codec: Kvern.Codec.Edn
    #   )
    #   pid
    # end
    # store = launch.()
    # key = "edn-key-1"
    # val = %{
    #   test: "value",
    #   submap: %{"a" => 1, "b" => 2, 123 => 4.5, [:list, As, "key"] => {:my_tuple_tag, "val"}},
    # }
    # assert :ok === Kvern.put!(store, key, val)
    # Kvern.shutdown(store)
    # store = launch.()
    # assert ^val = Kvern.fetch!(store, key)
  end

  test "EDN binaries" do
    IO.puts "@todo This test belongs to xdn repo"
    # md5 = Xdn.module_info(:md5)
    # assert md5 ===
    #   md5 |> Xdn.encode! |> Xdn.decode!
  end

end
