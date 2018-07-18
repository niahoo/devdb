defmodule DevDBTest do
  use ExUnit.Case, async: true

  @db1 __MODULE__

  @dbtores_conf %{
    @db1 => []
  }

  defp start_db(db) do
    conf = Map.get(@dbtores_conf, db)
    IO.puts("start with conf : #{inspect(conf)}")
    DevDB.start_link(db, conf)
  end

  test "start/stop the database" do
    {:ok, pid} = start_db(@db1)
    assert is_pid(pid)
    DevDB.stop(pid, 1000)
  end

  test "set/get a value" do
    {:ok, pid} = start_db(@db1)
    assert :ok = DevDB.put(pid, "key", 1234)
    assert {:ok, 1234} = DevDB.fetch(pid, "key")
  end
end
