defmodule Kvern.Codec.Edn do
  @behaviour Kvern.Codec
  def extension,
    do: "edn"

  def encode(data),
    do: Xdn.encode(data, pretty: true, iodata: false)

  @todo "Handle user tag handlers"
  def decode(string),
    do: Xdn.decode(string)
end
