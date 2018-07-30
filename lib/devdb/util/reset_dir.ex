defmodule DevDB.Util.ResetDir do
  require Logger

  def reset_dir!(dir, log \\ false) do
    if log do
      Logger.debug("Remove directory #{dir} ...")
    end

    File.rm_rf!(dir)
    wait_dir_exists(dir, false, 2000)

    if log do
      Logger.debug("Ok.")
      Logger.debug("Create directory #{dir} ...")
    end

    File.mkdir_p!(dir)

    if log do
      Logger.debug("Ok.")
    end

    # wait_dir_exists sleeps 100 ms
    wait_dir_exists(dir, true, 2000)
    dir
  end

  @ms_gap 10

  # Wait dir exists

  defp wait_dir_exists(dir, should_exist, remain),
    do: wait_dir_exists(dir, File.dir?(dir), should_exist, remain)

  defp wait_dir_exists(dir, should, remain) when remain < 1,
    do: raise("Could not create directory #{dir}")

  defp wait_dir_exists(_, should, should, _) do
    # exists === should so we are good
    :ok
  end

  defp wait_dir_exists(dir, _, should, remain) do
    Process.sleep(@ms_gap)
    wait_dir_exists(dir, File.dir?(dir), should, remain - @ms_gap)
  end
end
