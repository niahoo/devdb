defmodule Kvern.Codec do
  @callback encode!(keyval :: Map.t(), opts :: [any()]) :: binary()
  @callback decode!(file_content :: binary(), opts :: [any()]) :: Map.t()

  @m __MODULE__
  @default_ext ".kv"

  defstruct mod: nil, enc_opts: [], dec_opts: [], ext: @default_ext

  def configure(module) when is_atom(module) do
    %@m{mod: module}
  end

  def configure(opts) when is_list(opts) do
    %@m{
      mod: Keyword.fetch!(opts, :module),
      enc_opts: Keyword.get(opts, :encode, []),
      dec_opts: Keyword.get(opts, :decode, []),
      ext: Keyword.get(opts, :ext, @default_ext)
    }
  end

  def encode!(%@m{mod: mod, enc_opts: opts}, term) do
    mod.encode!(term, opts)
  end

  def decode!(%@m{mod: mod, dec_opts: opts}, term) do
    mod.decode!(term, opts)
  end

  def ext(%@m{ext: ext}), do: ext
end
