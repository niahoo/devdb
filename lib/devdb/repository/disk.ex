defmodule DevDB.Repository.Disk do
  require Logger
  use TODO
  defstruct dir: nil, codec: nil, file_ext: nil
  alias Kvern.Codec

  @b64_prefix "b64."
  @re_valid_filename ~r/^[\P{C}]+$/

  def new(opts) do
    dir = opts[:dir]

    codec =
      (opts[:codec] || __MODULE__)
      |> Codec.configure()

    unless File.dir?(dir) do
      raise "Not a directory : #{inspect(dir)}"
    end

    %__MODULE__{dir: dir, codec: codec, file_ext: Codec.ext(codec)}
  end

  def reset_dir!(dir, log \\ false) do
    if log do
      Logger.debug("Remove directory #{dir} ...")
    end

    File.rm_rf!(dir)

    if log do
      Logger.debug("Ok.")
      Logger.debug("Create directory #{dir} ...")
    end

    File.mkdir_p!(dir)

    if log do
      Logger.debug("Ok.")
    end

    # wait_dir_exists sleeps 100 ms
    second = 10
    wait_dir_exists(dir, second * 2)
  end

  defp wait_dir_exists(dir, 0) do
    raise "Could not create directory #{dir}"
  end

  defp wait_dir_exists(dir, stop) do
    if File.dir?(dir) do
      IO.puts("dir resetted #{dir}")
      dir
    else
      Process.sleep(100)
      wait_dir_exists(dir, stop - 1)
    end
  end

  def key_to_filename(key, ext) when is_binary(key) do
    if Regex.match?(@re_valid_filename, key) do
      key
    else
      @b64_prefix <> Base.url_encode64(key, padding: false)
    end <> ext
  end

  def key_to_filename(key, ext) do
    key
    |> :erlang.term_to_binary()
    |> key_to_filename(ext)
  end

  def key_to_filename(key, ext, dir), do: Path.join(dir, key_to_filename(key, ext))

  def put(state, key, value) do
    binary = Codec.encode!(state.codec, value)

    ext = state.file_ext
    path = key_to_filename(key, ext, state.dir)

    # Logger.debug("Write to disk : #{inspect(key)}")
    File.write!(path, binary, [:raw])
  end

  ## -- Codec behaviour

  def encode!(term, _opts), do: encode!(term)
  def decode!(term, _opts), do: decode!(term)

  defdelegate decode!(binary), to: :erlang, as: :binary_to_term
  defdelegate encode!(binary), to: :erlang, as: :term_to_binary
end

## --

## --

## --

defimpl DevDB.Repository.Store, for: DevDB.Repository.Disk do
  import DevDB.Repository.Entry
  require Logger
  alias DevDB.Repository.Disk
  import Disk
  alias Kvern.Codec

  # We only use key and value, so we match-out any transactional reference
  def put_entry(state, db_entry(key: key, value: value, trref: nil)) do
    Disk.put(state, key, value)
  end

  def delete(state, key) do
    path = key_to_filename(key, state.file_ext, state.dir)

    case File.rm(path) do
      :ok ->
        state

      {:error, :enoent} ->
        Logger.debug("Could not delete file #{path}, :enoent")
        state

      e ->
        # We re-trigger the exception
        Logger.error("Could not delete file #{path}, #{inspect(e)}")
        File.rm!(path)
        # still reply with the state in case it suddenly works to rm the file
        state
    end
  end

  def fetch(state, key) do
    key
    |> key_to_filename(state.file_ext, state.dir)
    |> read_file(state.codec)
    |> case do
      {:ok, [@vtag, ^key, value]} ->
        {:ok, value}

      {:error, {:enonent, _}} ->
        # No file, key does not exist
        :error

      {:error, reason} ->
        Logger.error("Could not fetch file for key #{inspect(key)}, reason: #{inspect(reason)}")
        :error
    end
  end

  def keys(_state) do
    # As some keys must be encoded in base64, in order to get all the keys we
    # must read all the files to get the actual keys inside them
    raise "Unavailable"
  end

  def read_file(path, codec) do
    if File.exists?(path) do
      decode_file(path, codec)
    else
      {:error, {:enonent, path}}
    end
  end

  def decode_file(path, codec) do
    try do
      ext = Codec.ext(codec)
      basename = Path.basename(path)
      binary = File.read!(path)
      term = Codec.decode!(codec, binary)

      case term do
        [@vtag, key, _value] = data ->
          if key_to_filename(key, ext) === basename do
            {:ok, data}
          else
            {:error, {{:key_filename_mismatch, key, key_to_filename(key, ext)}, path}}
          end

        _other ->
          # This is BEAM data but not a 2-tuple with the key
          {:error, {:data_mismatch, path}}
      end
    rescue
      e -> {:error, {e, path}}
    end
  end
end
