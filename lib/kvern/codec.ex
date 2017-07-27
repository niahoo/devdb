defmodule Kvern.Codec do
  @callback extension() :: String.t
  @callback encode(any, opts :: Keyword.t) :: {:ok, String.t} | {:error, any}
  @callback decode(source :: String.t, opts :: Keyword.t) :: {:ok, any} | {:error, any}
end
