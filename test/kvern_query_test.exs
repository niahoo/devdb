defmodule KvernQueryTest do
  use ExUnit.Case, async: false

  @dir_queries File.cwd!() |> Path.join("test/stores/d3-queries")
  @s3 __MODULE__

  @stores_conf %{
    @s3 => [disk_copy: @dir_queries]
  }
  setup_all do
    Kvern.Repo.Disk.reset_dir(@dir_queries)
    Application.ensure_started(:kvern)
    launch_store(@s3)
    :ok
  end

  def launch_store(store) do
    conf = Map.fetch!(@stores_conf, store)
    {:ok, _} = Kvern.open(store, conf)
  end

  def seed(store) do
    :ok = Kvern.nuke(store)
    :ok = Kvern.put!(store, "a", {:group_1, :team_3})
    :ok = Kvern.put!(store, "b", {:group_1, :team_3})
    :ok = Kvern.put!(store, "c", {:group_1, :team_4})
    :ok = Kvern.put!(store, "e", {:group_1, :team_4})
    :ok = Kvern.put!(store, "f", {:group_2, :team_5})
    :ok = Kvern.put!(store, "g", {:group_2, :team_5})
    :ok = Kvern.put!(store, "h", {:group_2, :team_6})
    :ok = Kvern.put!(store, "i", {:group_2, :team_6})
  end

  test "simple select" do
    seed(@s3)

    {:ok, selection} =
      Kvern.select(@s3, fn
        {_, {:group_2, _}} -> true
        _ -> false
      end)

    found_keys = select_group_keys_sorted(@s3, :group_2)

    assert found_keys === ["f", "g", "h", "i"]
  end

  test "transaction select" do
    seed(@s3)

    Kvern.transaction(@s3, fn ->
      Kvern.put(@s3, "g", "something else")
      Kvern.put(@s3, "h", "something else")

      found_keys_inside = select_group_keys_sorted(@s3, :group_2)

      assert found_keys_inside === ["h", "i"]
    end)

    found_keys_outside = select_group_keys_sorted(@s3, :group_2)
    assert found_keys_outside === ["f", "g", "h", "i"]
  end

  defp select_group_keys_sorted(store, group) do
    {:ok, selection} =
      Kvern.select(store, fn
        {_, {^group, _}} -> true
        _ -> false
      end)

    selection
    |> Keyword.keys()
    |> IO.inspect()
    |> Enum.sort()
  end
end
