defmodule Sidekick do
  defstruct me: nil, other: nil

  def spawn_link(fun) when is_function(fun, 0) do
    raise """
    Sidekick.spawn_link accept only a callback with arity of 1.
    The callback is given a data structure to control synchronisation.
    """
  end

  def spawn_link(fun) when is_function(fun, 1) do
    spawn(fun, :link)
  end

  def spawn(fun, link \\ nil) when is_function(fun, 1) do
    init = fn ->
      receive do
        {__MODULE__, infos} ->
          # Before giving the infos to the sidekick we swap me/other to be
          # correct
          %{me: other, other: me} = infos
          infos = %{infos | me: me, other: other}
          fun.(infos)
      end
    end

    sk_pid =
      case link do
        :link ->
          Kernel.spawn_link(init)

        _ ->
          Kernel.spawn(init)
      end

    infos = %__MODULE__{me: self(), other: sk_pid}
    send(sk_pid, {__MODULE__, infos})
    {:ok, infos}
  end

  def join(%{other: other}, term) do
    # to sync, we just send the info and then wait for the info
    send(other, term)

    receive do
      ^term -> :ok
    end
  end

  def pid_of(%{other: other}), do: other

end
