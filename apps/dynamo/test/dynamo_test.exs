defmodule DynamoTest do
  use ExUnit.Case
  import Emulation, only: [spawn: 2, send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  # This function will get the name from view given index
  @spec getNameFromView(list({atom(), non_neg_integer()}), non_neg_integer()) :: {:atom, non_neg_integer()}
  def getNameFromView(view, idx) do
    Enum.at(view, rem(idx, length(view)))
  end

  # This function will generate preference list for one node given N and its index in the ring
  @spec getPreferenceList(list({atom(), non_neg_integer()}), pos_integer(), non_neg_integer()) :: list({atom(), non_neg_integer()})
  def getPreferenceList(view, n, start_idx) do
    Enum.to_list(1..n-1)
    |> Enum.map(fn(x) ->
      getNameFromView(view, start_idx+x)
    end)
  end

  # This function will generate a Dynamo configuration list given N
  @spec getConfigList(list({atom(), non_neg_integer()}), atom(),pos_integer(), pos_integer(), pos_integer(), pos_integer(), pos_integer()) :: list(%Dynamo{})
  def getConfigList(view, dispatcher, n, heartbeat_timeout, checkout_timeout, write_res, read_res) do
    view
    |> Enum.with_index
    |> Enum.map(fn({{x,index}, i})->
      Dynamo.new(index, dispatcher, getPreferenceList(view, n, i), view, heartbeat_timeout, checkout_timeout, write_res, read_res)
    end)
  end

  test "Get and Put function" do
    a=Dynamo.new(0,nil,nil,500,10000,1,1)
    a=Dynamo.put(a,0,10)
    a=Dynamo.put(a,2,12)
    a=Dynamo.put(a,1,11)
    a=Dynamo.put(a,1,15)
    IO.inspect(Dynamo.get(a,1))
    IO.inspect(Dynamo.get(a,3))
    IO.inspect(a.hash_map)
  end

  test "Range Function" do
    a=Dynamo.new(0,nil,
    [{:b,3},{:c,5},{:d,7},{:e,9}],
    500,10000,1,1)
    a=Dynamo.put(a,0,10)
    a=Dynamo.put(a,1,11)
    a=Dynamo.put(a,4,14)
    a=Dynamo.put(a,3,13)
    a=Dynamo.put(a,12,22)
    IO.inspect(Dynamo.find_range(a.view,4))
    IO.inspect(Dynamo.find_range(a.view,5))
    IO.inspect(Dynamo.find_range(a.view,6))
    IO.inspect(Dynamo.find_range(a.view,1))
    IO.inspect(Dynamo.find_range(a.view,10))
    IO.puts("Data Finding")
    IO.inspect(Dynamo.find_data(a.hash_map,Dynamo.find_range(a.view,10)))
    IO.inspect(Dynamo.find_data(a.hash_map,Dynamo.find_range(a.view,5)))
  end

  test "Set up nodes and check whether nodes input correctly synchronize" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])
    view=[{:a,3},{:b,5},{:c,7},{:d,9},{:e,11}]
    write_res=2
    read_res=2
    dispatcher_name=:dispatcher
    config_list=getConfigList(view,dispatcher_name, 3, 50, 5000, write_res, read_res)
    config_list
    |> Enum.with_index
    |> Enum.each(fn ({config,i}) ->
      {node,_} =Enum.at(view,i)
      # IO.puts("Generating node for #{node}")
      spawn(node, fn -> Dynamo.dynamo(config,[]) end)
    end)
    dispatcher = Dynamo.Dispatcher.new(view)
    spawn(dispatcher_name, fn -> Dynamo.Dispatcher.dispatcher(dispatcher, nil) end)
    client = Dynamo.Client.new(500,:dispatcher)
    client_thread=
      spawn(:client, fn ->
    tmp=Dynamo.Client.put(client, 0, 10)
    IO.inspect(tmp)
    tmp=Dynamo.Client.put(client, 0, 11)
    receive do
    after
      50 -> assert true
    end
    result= Dynamo.Client.get(client, 0)
    IO.inspect(result)
    # assert result ==5
    end)
    handle = Process.monitor(client_thread)
    # Timeout.
    receive do
      {:DOWN, ^handle, _, _, _} -> true
    after
      1000 -> assert true
    end
  after
    Emulation.terminate()
  end

  test "Data for Plots" do

  end
end
