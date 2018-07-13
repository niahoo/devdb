defmodule Kvern.Repo.Disk do
  require Logger
  use TODO
  defstruct dir: nil
  import :erlang, only: [{:binary_to_term, 1}, {:term_to_binary, 1}]

  @behaviour Kvern.Seed
  @behaviour Kvern.Repo

  @ext ".bin"
  @b64_prefix "b64."
  @re_valid_file ~r/^[\P{C}]+$/

  def new(opts) do
    dir = opts[:dir]

    unless File.dir?(dir) do
      raise "Not a directory : #{inspect(dir)}"
    end

    %__MODULE__{dir: dir}
  end

  # seed behaviour
  def init(opts), do: new(opts)

  def delete(state, key) do
    path = key_to_filename(key, state.dir)

    case File.rm(path) do
      :ok ->
        state

      {:error, :enoent} ->
        state

      _other ->
        # We re-trigger the exception
        File.rm!(path)
        # still reply with the state in case it suddenly works to rm the file
        state
    end
  end

  def fetch(state, key) do
    key
    |> key_to_filename(state.dir)
    |> read_file()
  end

  def keys(_state) do
    # As some keys must be encoded in base64, in order to get all the keys we
    # must read all the files to get the actual keys inside them
    raise "Unavailable"
  end

  def nuke(state) do
    reset_dir(state.dir)
    state
  end

  def put(state, key, value) do
    key
    |> key_to_filename(state.dir)
    |> File.write!(term_to_binary({key, value}), [:raw])

    state
  end

  def stream_updates(%__MODULE__{dir: dir}) do
    dir
    |> File.ls!()
    |> Stream.map(&Path.join(dir, &1))
    |> Stream.map(&read_file/1)
    |> Stream.filter(fn
      {:ok, {_, _}} ->
        true

      {:error, {reason, file}} ->
        Logger.warn("Could not read file #{file} as a Kvern entity, reason: #{inspect(reason)}")
        false
    end)
    |> Stream.map(&to_update_format!/1)
  end

  def to_update_format!({:ok, {key, val}}), do: {:put, key, val}

  def read_file(path) do
    try do
      basename = Path.basename(path)

      path
      |> File.read!()
      |> binary_to_term()
      |> case do
        {key, _} = data ->
          if key_to_filename(key) === basename do
            {:ok, data}
          else
            {:error, {{:key_filename_mismatch, key, key_to_filename(key)}, path}}
          end

        _other ->
          # This is BEAM data but not a 2-tuple with the key
          {:error, {:data_mismatch, path}}
      end
    rescue
      e -> {:error, {e, path}}
    end
  end

  def key_to_filename(key) when is_binary(key) do
    if Regex.match?(@re_valid_file, key) do
      key
    else
      @b64_prefix <> Base.url_encode64(key, padding: false)
    end <> @ext
  end

  def key_to_filename(key) do
    key
    |> to_string()
    |> key_to_filename()
  end

  def key_to_filename(key, dir), do: Path.join(dir, key_to_filename(key))

  defp reset_dir(dir) do
    # File.rm_rf!(dir)
    File.mkdir_p!(dir)
  end
end
