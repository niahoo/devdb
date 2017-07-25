defmodule Kvern.Codec.Edn do
  @behaviour Kvern.Codec
  def extension,
    do: "edn"

  def encode(data, options),
    do: Xdn.encode(data, options)

  def decode(string, options),
    do: Xdn.decode(string, options)
end
