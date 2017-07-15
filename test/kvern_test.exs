defmodule KvernTest do
  use ExUnit.Case
  doctest Kvern

  @store __MODULE__
  @dir_1 (
    File.cwd!
    |> Path.join("stores/d1")
  )

  setup_all do
    File.mkdir_p! @dir_1
    Application.ensure_started(:kvern)
    assert {:ok, pid} = Kvern.open(name: @store, path: @dir_1)
    assert is_pid(pid)
    :ok
  end


  test "key format" do
    assert :ok = Kvern.valid_key?("a")
    assert :ok = Kvern.valid_key?("aqz009_-___zza")
    assert :ok = Kvern.valid_key?("A")

    assert {:error, _} = Kvern.valid_key?("0ab")
    assert {:error, _} = Kvern.valid_key?("1")
    assert {:error, _} = Kvern.valid_key?("é")
    assert {:error, _} = Kvern.valid_key?("hàça")

    assert :ok = Kvern.valid_key?("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    assert {:error, _} = Kvern.valid_key?("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
  end

  test "put / get simple value" do
    key = "mykey"
    val = :some_value
    assert :ok === Kvern.put!(@store, key, val)
    Kvern.print_dump(@store)
    assert :error === Kvern.fetch(@store, "__no_exist__")
    assert nil === Kvern.get(@store, "__no_exist__")
    assert :__hey__ === Kvern.get(@store, "__no_exist__", :__hey__)
    recup = Kvern.get(@store, key)
    assert recup === val
  end

  test "simple transaction" do
    key = "tkey"
    val = :some_value
    assert :ok === Kvern.put!(@store, key, val)
    assert :ok === Kvern.begin(@store)
    assert :ok === Kvern.put!(@store, key, "__some_other_value__")
    assert "__some_other_value__" === Kvern.get(@store, key)
    assert :ok === Kvern.rollback(@store)
    assert val === Kvern.get(@store, key)
  end

end
