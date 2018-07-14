defmodule KvernQueryTest do
  use ExUnit.Case, async: false

  @store __MODULE__

  @dir_queries File.cwd!() |> Path.join("test/stores/d3-queries")

  setup_all do
    Kvern.Repo.Disk.reset_dir(@dir_queries)
    Application.ensure_started(:kvern)
    launch_store()
    :ok
  end

  def launch_store() do
    launch_store(@store, @dir_queries)
  end

  def launch_store(store, dir, codec \\ nil) do
    disk_copy = [dir: dir, codec: codec]
    {:ok, _} = Kvern.open(store, disk_copy: disk_copy)
    :ok = Kvern.nuke(store)
    :ok = Kvern.put!(store, "a", {:group_1, :group_3})
    :ok = Kvern.put!(store, "b", {:group_1, :group_3})
    :ok = Kvern.put!(store, "c", {:group_1, :group_4})
    :ok = Kvern.put!(store, "e", {:group_1, :group_4})
    :ok = Kvern.put!(store, "f", {:group_2, :group_5})
    :ok = Kvern.put!(store, "g", {:group_2, :group_5})
    :ok = Kvern.put!(store, "h", {:group_2, :group_6})
    :ok = Kvern.put!(store, "i", {:group_2, :group_6})
  end

  @tag :skip
  test "simple query" do
  end
end
