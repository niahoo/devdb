defmodule DevDB.Util.ResetDir do
  require Logger

  def reset_dir!(dir, log \\ false) do
    if log do
      Logger.debug("Remove directory #{dir} ...")
    end

    File.rm_rf!(dir)
    wait_dir_removed(dir, 2000)

    if log do
      Logger.debug("Ok.")
      Logger.debug("Create directory #{dir} ...")
    end

    File.mkdir_p!(dir)

    if log do
      Logger.debug("Ok.")
    end

    # wait_dir_exists sleeps 100 ms
    wait_dir_exists(dir, 2000)
    IO.puts("dir resetted #{dir}")
    dir
  end

  @ms_gap 10

  # Wait dir exists

  defp wait_dir_exists(dir, remain), do: wait_dir_exists(dir, File.dir?(dir), remain)

  defp wait_dir_exists(dir, remain) when remain < 1,
    do: raise("Could not create directory #{dir}")

  defp wait_dir_exists(_, true, _), do: :ok

  defp wait_dir_exists(dir, false, remain) do
    Process.sleep(@ms_gap)
    wait_dir_removed(dir, File.dir?(dir), remain - @ms_gap)
  end

  # Wait dir removed

  defp wait_dir_removed(dir, remain), do: wait_dir_removed(dir, File.dir?(dir), remain)

  defp wait_dir_removed(dir, remain) when remain < 1,
    do: raise("Could not create directory #{dir}")

  defp wait_dir_removed(_, false, _), do: :ok

  defp wait_dir_removed(dir, true, remain) do
    Process.sleep(@ms_gap)
    wait_dir_removed(dir, File.dir?(dir), remain - @ms_gap)
  end
end
