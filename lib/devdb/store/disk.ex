defmodule DevDB.Store.Disk do
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

  def

  def put(state, key, value) do
    binary = Codec.encode!(state.codec, value)

    ext = state.file_ext
    path = key_to_filename(key, ext, state.dir)

    # Logger.debug("Write to disk : #{inspect(key)}")
    File.write!(path, binary, [:raw])
  end

  def reduce_all(%__MODULE__{dir: dir, codec: codec, file_ext: ext}, acc, fun) do
    dir
    |> File.ls!()
    |> Stream.map(&Path.join(dir, &1))
    |> Stream.map(&file_to_kv(&1, ext, codec))
    |> Enum.reduce()
  end

  defp file_to_kv(path, ext, codec) do
    binkey = Path.basename(path, ext)
    key = basename_to_key(binkey)
    content = File.read!(path)
    value = Codec.decode!(codec, content)
    {key, value}
  end

  ## -- Codec behaviour

  def encode!(term, _opts), do: encode!(term)
  def decode!(term, _opts), do: decode!(term)

  defdelegate decode!(binary), to: :erlang, as: :binary_to_term
  defdelegate encode!(binary), to: :erlang, as: :term_to_binary

  ## -- Util

  defdelegate reset_dir!(dir), to: DevDB.Util.ResetDir
  defdelegate reset_dir!(dir, log), to: DevDB.Util.ResetDir
end

## --

## --

## --

defimpl DevDB.Store, for: DevDB.Store.Disk do
  import DevDB.Entry
  require Logger
  alias DevDB.Store.Disk
  import Disk
  alias Kvern.Codec

  # We only use key and value, so we match-out any transactional reference
  def put_entry(state, db_entry(key: key, value: value, trref: nil)) do
    Disk.put(state, key, value)
  end

  def delete_entry(state, key) do
    path = key_to_filename(key, state.file_ext, state.dir)

    :ok = File.rm!(path)
  end

  def reduce_entries(store, acc, fun) do
    Dis.reduce_all(store, acc, fn disk_entry, acc ->
      {key, value} = disk_entry
      entry = db_entry(key: key, value: value)
      fun.(entry, acc)
    end)
  end
end
