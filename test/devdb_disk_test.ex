defmodule DevDBDiskTest do
  use ExUnit.Case, async: true
  use TODO, print: :all

  @dbdisk __MODULE__

  @dir_default File.cwd!() |> Path.join("test/stores/db1") |> DevDB.Repository.Disk.reset_dir!()

  @dbtores_conf %{
    @dbdisk => [backend: DevDB.Repository.Disk.new(dir: @dir_default)]
  }

  defp start_db!(db) do
    conf = Map.get(@dbtores_conf, db)
    {:ok, pid} = DevDB.start_link(db, conf)
    true = is_pid(pid)
    pid
  end

  test "put a bunch of k/v in files" do
    pid = start_db!(@dbdisk)
    assert is_pid(pid)

    DevDB.put(pid, "my-string-key", ["some", "values", %{"with" => "stuff"}])
  end
end
