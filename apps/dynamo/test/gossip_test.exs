defmodule GossipTest do
  use ExUnit.Case
  import Emulation, only: [spawn: 2, send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  # import DynamoTest, only: [getConfigList: 6]

  # This function will get the name from view given index
  @spec getNameFromView(list({atom(), non_neg_integer()}), non_neg_integer()) :: {:atom, non_neg_integer()}
  defp getNameFromView(view, idx) do
    Enum.at(view, rem(idx, length(view)))
  end

  # This function will generate preference list for one node given N and its index in the ring
  @spec getPreferenceList(list({atom(), non_neg_integer()}), pos_integer(), non_neg_integer()) :: list({atom(), non_neg_integer()})
  defp getPreferenceList(view, n, start_idx) do
    Enum.to_list(1..n-1)
    |> Enum.map(fn(x) ->
      getNameFromView(view, start_idx+x)
    end)
  end

 # This function will generate a Dynamo configuration list given N
 @spec getConfigList(list({atom(), non_neg_integer()}), pos_integer(), pos_integer(), pos_integer(), pos_integer(), pos_integer()) :: list(%Dynamo{})
 def getConfigList(view, n, heartbeat_timeout, checkout_timeout, write_res, read_res) do
   view
   |> Enum.with_index
   |> Enum.map(fn({{x,index}, i})->
     Dynamo.new(index, getPreferenceList(view, n, i), view, heartbeat_timeout, checkout_timeout, write_res, read_res)
   end)
 end
  # # This function will generate a Dynamo configuration list given N
  # @spec getConfigList(list({atom(), non_neg_integer()}), pos_integer(), pos_integer(), pos_integer()) :: list(%Dynamo{})
  # defp getConfigList(view, n, heartbeat_timeout, checkout_timeout) do
  #   view
  #   |> Enum.with_index
  #   |> Enum.map(fn({x, i})->
  #     Dynamo.new(i, view, :dispatcher, getPreferenceList(view, n, i), heartbeat_timeout, checkout_timeout)
  #   end)

  # end

  @spec whetherNotInFailedNodes(list(atom()), atom()) :: boolean
  defp whetherNotInFailedNodes(failed_nodes, node) do
    failed_nodes |> Enum.find_index(fn x -> x==node end) == nil
  end


  test "Set up nodes and check whether system handle node failure correctly" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])
    view = [:A, :B, :C, :D, :E] |> Enum.with_index
    n = 3
    failed_nodes = [:A3, :A6]
    heartbeat_timeout = 500
    checkout_timeout = 5000
    config_list = getConfigList(view, n, heartbeat_timeout, checkout_timeout, 1, 1)
    config_list
    |> Enum.with_index
    |> Enum.each(fn({config, i}) ->
      {node, _} = Enum.at(view, i)
      IO.puts("Generating node for #{node}")
      spawn(node, fn -> Dynamo.setup_node(config) end)
    end)
    dispatcher = Dynamo.Dispatcher.new(view)
    spawn(:dispatcher, fn -> Dynamo.Dispatcher.dispatcher(dispatcher, nil) end)

    client = spawn(:client, fn ->
      failed_nodes |> Enum.each(fn(x) ->
        send(x, :to_dead)
      end)
      receive do
        {x, :received_to_dead} ->
          IO.puts("Client received to dead response from node #{x}")
          :ok
      end
      receive do
      after
        15_000 -> :ok
      end
      view |> Enum.each(fn ({x,_}) ->
        send(x, :get_failure_list)
      end)
      failure_lists =
          view
          |> Enum.map(fn ({x, _}) ->
            receive do
              {^x, %Dynamo.ClientFailureNodeListMessage{failure_node_list: failure_list}} ->
                IO.puts("Received failure list from #{x}")
                failure_list
            end
          end)
      failure_lists
      |> Enum.with_index
      |> Enum.each(fn({l, idx}) ->
        {node, _} = getNameFromView(view, idx)
        if(node != :C) do
          IO.puts("Checking failure list for node: #{node}")
          assert l == MapSet.new([:C])
        # else
        #   assert l==MapSet.new()
        end
      end)
      view |> Enum.each(fn ({x, _}) ->
        send(x, :get_pref_list)
      end)
      pref_lists =
        view
        |> Enum.map(fn ({x, _})->
          receive do
            {^x, %Dynamo.ClientPrefListMessage{pref_list: pref_list}} ->
                IO.puts("Received pref list from #{x}")
                pref_list
          end
        end)
      pref_lists
      |> Enum.with_index
      |> Enum.each(fn({l, idx})->
          {node, _} = getNameFromView(view, idx)
          if node != :C do
            current_view = view |> Enum.filter(fn ({x, _}) -> !Enum.member?(failed_nodes, x) end)
            idx = current_view |> Enum.find_index(fn ({x, _}) -> x == node end)
            assert l == getPreferenceList(current_view, n, idx)
          end
      end)
      send(:dispatcher, :getRangeNodeMap)
      range_node_map = receive do
        {:dispatcher, %Dynamo.ClientRangeNodeMapMessage{range_node_map: range_node_map}} ->
          range_node_map
      end
      assert range_node_map == view |> Enum.filter(fn({node, _}) -> node != :C end)

    end)
    handle = Process.monitor(client)
    # Timeout.
    receive do
      {:DOWN, ^handle, _, _, _} -> true
    after
      20_000 -> assert true
    end
  after
    Emulation.terminate()
  end

  test "Set up the nodes and check whether system handle multiple node failures correctly" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])
    view = [:A1, :A2, :A3, :A4, :A5, :A6, :A7, :A8] |> Enum.with_index
    n = 4
    failed_nodes = [:A3, :A6]
    heartbeat_timeout = 500
    checkout_timeout = 5000
    config_list = getConfigList(view, n, heartbeat_timeout, checkout_timeout, 1, 1)
    config_list
    |> Enum.with_index
    |> Enum.each(fn({config, i}) ->
      {node, _} = Enum.at(view, i)
      IO.puts("Generating node for #{node}")
      spawn(node, fn -> Dynamo.setup_node(config) end)
    end)
    dispatcher = Dynamo.Dispatcher.new(view)
    spawn(:dispatcher, fn -> Dynamo.Dispatcher.dispatcher(dispatcher, nil) end)

    client = spawn(:client, fn ->
      failed_nodes |> Enum.each(fn(x) ->
        send(x, :to_dead)
      end)
      receive do
        {x, :received_to_dead} ->
          IO.puts("Client received to dead response from node #{x}")
          :ok
      end
      receive do
      after
        10_000 -> :ok
      end
      view |> Enum.each(fn ({x, _}) ->
        send(x, :get_failure_list)
      end)
      failure_lists =
          view
          |> Enum.map(fn ({x, _}) ->
            receive do
              {^x, %Dynamo.ClientFailureNodeListMessage{failure_node_list: failure_node_list}} ->
                IO.puts("Received failure list from #{x}")
                failure_node_list
            end
          end)
      failure_lists
      |> Enum.with_index
      |> Enum.each(fn({l, idx}) ->
        {node, _} = getNameFromView(view, idx)
        if(!Enum.member?(failed_nodes, node)) do
          IO.puts("Checking failure list for node: #{node}")
          assert l==MapSet.new(failed_nodes)
        # else
        #   assert l==MapSet.new()
        end
      end)
      view |> Enum.each(fn ({x, _}) ->
        send(x, :get_pref_list)
      end)
      pref_lists =
        view
        |> Enum.map(fn ({x, _}) ->
          receive do
            {^x, %Dynamo.ClientPrefListMessage{pref_list: pref_list}} ->
                IO.puts("Received pref list from #{x}")
                pref_list
          end
        end)
      pref_lists
      |> Enum.with_index
      |> Enum.each(fn({l, idx})->
          {node, _} = getNameFromView(view, idx)
        if(!Enum.member?(failed_nodes, node)) do
          IO.inspect("Checking pref list for #{node}")
            current_view = view |> Enum.filter(fn ({x, _}) -> !Enum.member?(failed_nodes, x) end)
            idx = current_view |> Enum.find_index(fn ({x, _}) -> x == node end)
            assert l == getPreferenceList(current_view, n, idx)
        end
      end)

    end)
    handle = Process.monitor(client)
    # Timeout.
    receive do
      {:DOWN, ^handle, _, _, _} -> true
    after
      15_000 -> assert true
    end
  after
    Emulation.terminate()
  end


end
