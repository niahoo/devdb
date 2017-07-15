defmodule KvernTest do
  use ExUnit.Case, async: false
  doctest Kvern

  @store __MODULE__
  @mutex KvernTest.Mutex

  @dir_1 (
    File.cwd! |> Path.join("test/stores/d1")
  )

  setup_all do
    # use a mutex to linearize all tests
    children = [
      Mutex.child_spec(@mutex)
    ]
    {:ok, _pid} = Supervisor.start_link(children, strategy: :one_for_one)

    # setup Kvern
    File.rm_rf! @dir_1
    File.mkdir_p! @dir_1
    Application.ensure_started(:kvern)
    launch_store()
    :ok
  end

  def launch_store() do
    IO.puts "Launching store"
    {:ok, pid} = Kvern.open(
      name: @store,
      path: @dir_1,
      codec: Kvern.Codec.Exs
    )
    true = is_pid(pid)
    IO.puts "Launched store !"
    {:ok, pid}
  end

  test "key format" do
    assert :ok = Kvern.valid_key?("a")
    assert :ok = Kvern.valid_key?("aqz009_-___zza")
    assert :ok = Kvern.valid_key?("A")

    assert {:error, _} = Kvern.valid_key?("0ab")
    assert {:error, _} = Kvern.valid_key?("1")
    assert {:error, _} = Kvern.valid_key?("é")
    assert {:error, _} = Kvern.valid_key?("hàça")

    assert :ok = Kvern.valid_key?("aaaaaaaaaaaaaaaaaaaaaaaaa")
    assert {:error, _} = Kvern.valid_key?("aaaaaaaaaaaaaaaaaaaaaaaaaa")
  end

  test "put / get simple value" do
    lock()
    key = "mykey"
    val = :some_value
    assert :ok === Kvern.put!(@store, key, val)
    Kvern.print_dump(@store)
    assert :error === Kvern.fetch(@store, "__no_exist__")
    assert nil === Kvern.get(@store, "__no_exist__")
    assert :__hey__ === Kvern.get(@store, "__no_exist__", :__hey__)
    recup = Kvern.get(@store, key)
    assert recup === val
    goodbye()
  end

  test "simple transaction" do
    lock()
    key = "tkey"
    val = %{xyz: "This is some value"}
    assert :ok === Kvern.put!(@store, key, val)
    assert :ok === Kvern.begin(@store)
    assert :ok === Kvern.put!(@store, key, "__some_other_value__")
    assert "__some_other_value__" === Kvern.get(@store, key)
    assert :ok === Kvern.rollback(@store)
    assert val === Kvern.get(@store, key)
    goodbye()
  end

  test "call by pid" do
    lock()
    [{pid, _}] = Registry.lookup(Kvern.Registry, @store)
      |> IO.inspect
    assert is_pid(pid)
    assert :ok === Kvern.put(pid, "ignore", :ignore)
    goodbye()
  end

  test "restore" do
    lock()
    :ok = Kvern.nuke_storage(@store)
    :ok = Kvern.put(@store, "my-key-1", :my_value_1)
    :ok = Kvern.put(@store, "my-key-2", :my_value_2)
    Kvern.shutdown(@store)
    |> IO.inspect
    IO.puts "Store is down"
    {:ok, _pid} =
      launch_store()
      |> IO.inspect
    IO.puts "Store is up !"
    assert :my_value_1 = Kvern.fetch!(@store, "my-key-1")
    goodbye()
  end

  def lock() do
    Mutex.await(@mutex, @store)
    IO.puts "Locked !"
  end

  def goodbye() do
    Mutex.goodbye(@mutex)
    IO.puts "Lock released."
  end
end
