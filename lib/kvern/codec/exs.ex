# Inspired from the lib Eon
defmodule Kvern.Codec.Exs do
  def extension,
    do: "exs"

  def encode(data, _opts),
    do: {:ok, Macro.to_string(quote do: unquote(data)) <> "\n"}

  def decode(string, _opts) do
    {contents, _no_bindings} = Code.eval_string(string, [])
    {:ok, contents}
  end
end
