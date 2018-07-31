defmodule DevDBDiskTest do
  use ExUnit.Case, async: true
  use TODO, print: :all

  @dberl __MODULE__
  @dbsjon __MODULE__.JSON
  @dbbento __MODULE__.Bencode
  @dbmsgpack __MODULE__.MsgPack
  @dbseed __MODULE__.Shut
  @dbseedjson __MODULE__.ShutJson

  @dir_default File.cwd!()
               |> Path.join("test/stores/db-term2bin")
               |> DevDB.Store.Disk.reset_dir!()

  @dir_json File.cwd!()
            |> Path.join("test/stores/db-json")
            |> DevDB.Store.Disk.reset_dir!()

  @dir_bencode File.cwd!()
               |> Path.join("test/stores/db-bencode")
               |> DevDB.Store.Disk.reset_dir!()

  @dir_seed File.cwd!()
            |> Path.join("test/stores/db-seed")
            |> DevDB.Store.Disk.reset_dir!()

  @dir_seed_json File.cwd!()
                 |> Path.join("test/stores/db-seed-json")
                 |> DevDB.Store.Disk.reset_dir!()

  @dbs_conf %{
    @dberl => [backend: DevDB.Store.Disk.new(dir: @dir_default)],
    @dbsjon => [
      backend:
        DevDB.Store.Disk.new(
          dir: @dir_json,
          codec: [ext: ".json", module: Poison, encode: [pretty: true]]
        )
    ],
    @dbbento => [
      backend:
        DevDB.Store.Disk.new(
          dir: @dir_bencode,
          codec: [ext: ".btc", module: Bento, encode: [pretty: true]]
        )
    ],
    @dbseed => [
      backend: DevDB.Store.Disk.new(dir: @dir_seed),
      seed: :backend
    ],
    @dbseedjson => [
      backend:
        DevDB.Store.Disk.new(
          dir: @dir_seed_json,
          codec: [ext: ".json", module: Poison, encode: [pretty: true]]
        ),
      seed: :backend
    ]
  }

  defp start_db!(db) do
    conf = Map.get(@dbs_conf, db)
    {:ok, pid} = DevDB.start_link(conf)
    true = is_pid(pid)
    pid
  end

  test "put a bunch of k/v in files" do
    run_kv_inserts_with_store(@dberl, [
      # Here we are basically unit testing erlang term_to_binary/binary_to_term so
      # I'll not add much cases
      {"some-integer", 1_000_000_000_000_000_000_000_000_000_000_000_000_000},
      {"some-float", 0.123},
      {"some-nil", nil},
      {"some-nil-2", []},
      {"some-null", :null},
      {"some-float-2", 2.00000005},
      {"some-complicated", ["some", "values", %{"with" => "stuff"}]},
      # They key to filename conversion is more complicated !
      {{:tuple, "key"}, {:tuple, "key"}},
      {%{map: "key !"}, %{map: "key !"}},
      {'charlist-key', 'charlist-key'},
      {:atom, :atom},
      {true, true},
      {false, false},
      {nil, nil}
    ])

    run_kv_inserts_with_store(@dbsjon, [
      {"some-integer", 1},
      {"some-float", 0.123},
      {"some-float-2", 2.00000005},
      {"some-complicated", ["some", "values", %{"with" => "stuff"}]}
      # Json composed terms can only have string keys, so that's all
    ])

    run_kv_inserts_with_store(@dbbento, [
      # Bencode does not support float or atoms
      {"some-integer", 1},
      {"some-list", [1, 2, 3, 4]},
      {"some-mixed-or-nested-list", [1, "2", 'three']},
      {"some-complicated", ["some", "values", %{"with" => "stuff"}]},
      {"some-deep", [[[[[[[[[[[[[[[[[[[[[[[[[[<<0, 0, 0, 1>>]]]]]]]]]]]]]]]]]]]]]]]]]]}
    ])
  end

  defp run_kv_inserts_with_store(store_id, cases) do
    pid = start_db!(store_id)
    assert is_pid(pid)

    %DevDB.Store.Disk{dir: dir, codec: %{mod: mod, ext: ext}} = @dbs_conf[store_id][:backend]

    cases
    |> Enum.map(fn {key, val} ->
      DevDB.put(pid, key, val)
      filename = DevDB.Store.Disk.key_to_filename(key, ext)

      term =
        dir
        |> Path.join(filename)
        |> File.read!()
        |> mod.decode!([])

      assert term === val
    end)
  end

  test "Recover values after shutdown" do
    test_recover(@dbseed)
    test_recover(@dbseedjson)
  end

  def test_recover(store) do
    pid = start_db!(store)
    # Init data
    pairs = [
      {"a", 1},
      {"b", 2},
      {:atom, "test"},
      {{"iama", :tuple}, "val"},
      {%{map: true}, [true, false, nil]},
      {:will_delete, "I disappear"}
    ]

    Enum.each(pairs, fn {k, v} ->
      DevDB.put(pid, k, v)
    end)

    DevDB.delete(pid, :will_delete)

    # Stop the database
    assert :ok = DevDB.stop(pid)
    refute Process.alive?(pid)
    # We will start again, and the database must be populated with what was
    # stored on disk
    pid = start_db!(store)

    Enum.each(pairs, fn
      {:will_delete, _} ->
        # ignore
        :ok

      {k, v} ->
        assert v === DevDB.get(pid, k)
    end)

    assert :__default_val === DevDB.get(pid, :will_delete, :__default_val)
  end

  test "Recover after commit" do
    store = @dbseed
    pid = start_db!(store)
    k = "k_tr_commit"
    v = 1

    DevDB.transaction(pid, fn repo ->
      DevDB.put(repo, k, v)
    end)

    :ok = DevDB.stop(pid)
    pid = start_db!(store)
    assert v === DevDB.get(pid, k)
  end
end
