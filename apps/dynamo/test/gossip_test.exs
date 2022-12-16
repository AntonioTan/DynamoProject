defmodule GossipTest do
  use ExUnit.Case
  import Emulation, only: [spawn: 2, send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  # This function will get the name from view given index
  @spec getNameFromView(list(atom()), non_neg_integer()) :: :atom
  defp getNameFromView(view, idx) do
    Enum.at(view, rem(idx, length(view)))
  end

  # This function will generate preference list for one node given N and its index in the ring
  @spec getPreferenceList(list(atom()), pos_integer(), non_neg_integer()) :: list(atom())
  defp getPreferenceList(view, n, start_idx) do
    Enum.to_list(1..n-1)
    |> Enum.map(fn(x) ->
      getNameFromView(view, start_idx+x)
    end)
  end

  # This function will generate a Dynamo configuration list given N
  @spec getConfigList(list(atom()), pos_integer(), pos_integer(), pos_integer()) :: list(%Dynamo{})
  defp getConfigList(view, n, heartbeat_timeout, checkout_timeout) do
    view
    |> Enum.with_index
    |> Enum.map(fn({x, i})->
      Dynamo.new(i, getPreferenceList(view, n, i), heartbeat_timeout, checkout_timeout)
    end)

  end


  test "Set up nodes and check whether system handle node failure correctly" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])
    view = [:a, :b, :c, :d, :e]
    n = 3
    heartbeat_timeout = 1000
    checkout_timeout = 3000
    config_list = getConfigList(view, n, heartbeat_timeout, checkout_timeout)
    config_list
    |> Enum.with_index
    |> Enum.each(fn({config, i}) ->
      spawn(Enum.at(view, i), fn -> Dynamo.setup_node(config) end)
    end)
    client = spawn(:client, fn ->
      send(:c, :to_dead)
      receive do
        {:c, :received_to_dead} ->
          IO.puts("Client received to dead response from node c")
          :ok
      end
      receive do
      after
        4_000 -> :ok
      end
      view |> Enum.each(fn(x) ->
        send(x, :get_failure_list)
      end)
      failure_lists =
          view
          |> Enum.map(fn x ->
            receive do
              {^x, failure_list} -> failure_list
            end
          end)
      failure_lists |> Enum.each(fn(l) ->
        assert l==MapSet.new([:c])
      end)

    end)

  end

end
