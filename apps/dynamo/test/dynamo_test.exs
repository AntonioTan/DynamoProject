defmodule DynamoTest do
  use ExUnit.Case
  import Emulation, only: [spawn: 2, send: 2, whoami: 0]

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
    a=Dynamo.new(0,:dispatcher, nil,nil,500,10000,1,1)
    a=Dynamo.put(a,0,10)
    a=Dynamo.put(a,2,12)
    a=Dynamo.put(a,1,11)
    a=Dynamo.put(a,1,15)
    IO.inspect(Dynamo.get(a,1))
    IO.inspect(Dynamo.get(a,3))
    IO.inspect(a.hash_map)
  end

  test "Range Function" do
    a=Dynamo.new(0,:dispatcher, nil,
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
    # IO.inspect(tmp)
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
    view = [:A, :B, :C, :D, :E, :F, :G, :H, :I, :J, :K, :L] |> Enum.with_index
    n = 7
    w = 1
    r = 4
    interval_num = 4
    test_times = 3
    dispatcher_name = :dispatcher
    constant_interval = 10
    Enum.to_list(0..interval_num) |> Enum.map(
      fn(interval_idx) ->
        single_version_num = Enum.to_list(1..test_times) |> Enum.map(
          fn(test_time) ->
            whetherSame = true
            IO.puts("Starting the #{test_time} test with interval #{interval_idx*constant_interval}.....")
            Emulation.init()
            Emulation.append_fuzzers([Fuzzers.delay(10)])
            owner = self()
            time_interval = constant_interval*interval_idx
            config_list=getConfigList(view, dispatcher_name, n, 50, 5000, w, r)
            config_list
            |> Enum.with_index
            |> Enum.each(fn ({config,i}) ->
              {node,_} =Enum.at(view,i)
              # IO.puts("Generating node for #{node}")
              spawn(node, fn -> Dynamo.dynamo(config,[]) end)
            end)
            dispatcher = Dynamo.Dispatcher.new(view)
            spawn(:dispatcher, fn -> Dynamo.Dispatcher.dispatcher(dispatcher, nil) end)
            client = Dynamo.Client.new(500,:dispatcher)
            client_thread=
                spawn(:client, fn ->
                tmp=Dynamo.Client.put(client, 0, 10)
                tmp=Dynamo.Client.put(client, 0, 11)
                # tmp=Dynamo.Client.put(client, 0, 12)
                # tmp=Dynamo.Client.put(client, 0, 13)
                # tmp=Dynamo.Client.put(client, 0, 14)
                # IO.inspect(tmp)
                receive do
                end
              end)
            Process.sleep(time_interval)
            client2 = Dynamo.Client.new(500,:dispatcher)
            client_thread2 =
                spawn(:client2, fn ->
                {val, res} = Dynamo.Client.get(client2, 0)
                IO.puts("The value is #{res}")
                # assert result ==5
                send(owner, {:get_res, res})
                receive do
                  _ -> whetherSame = res
                end
              end)

            handle = Process.monitor(client_thread)
            # Timeout.
            receive do
              {:DOWN, ^handle, _, _, _} -> true
              # :heartbeat ->
              #   IO.puts("Received heartbeat")
            after
              2000 ->
                assert true
            end
            Emulation.terminate()
            whetherSame = receive do
              {:get_res, res} ->
                IO.puts("Received #{res}")
                res
            end
            whetherSame
          end
        ) |> Enum.filter(fn(x) -> x == true end) |> Enum.count()
        single_version_num * 1.0 / (test_times * 1.0)
      end
    ) |> Enum.with_index |> Enum.each(fn({single_version_rate, idx}) ->
      IO.puts("With time interval: #{constant_interval*idx}, the rate of single version storage is: #{single_version_rate}")
    end)
  # after
    # Emulation.terminate()

  end

  test "Spawn two processes with the same name" do
    Emulation.init()
    Emulation.append_fuzzers([Fuzzers.delay(2)])
    view=[{:a,3},{:b,5},{:c,7},{:d,9},{:e,11}]
    dispatcher = Dynamo.Dispatcher.new(view)
    spawn(:dispatcher, fn -> Dynamo.Dispatcher.dispatcher(dispatcher, nil) end)
    Emulation.init()
    spawn(:dispatcher, fn -> Dynamo.Dispatcher.dispatcher(dispatcher, nil) end)
  after
    Emulation.terminate()
  end
end
