defmodule Kvern.Codec do
  def extension,
    do: "edn"

  def encode(data),
    do: {:ok, inspect(data)}
end
