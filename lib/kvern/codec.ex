defmodule Kvern.Codec do
  @callback extension() :: String.t
  @callback encode(any) :: {:ok, String.t} | {:error, any}
  @callback decode(String.t) :: {:ok, any} | {:error, any}
end
