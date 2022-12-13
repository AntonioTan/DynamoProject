defmodule Dynamo do
  import Emulation, only: [send: 2, timer: 1, now: 0, whoami: 0]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  require Fuzzers
  # This allows you to use Elixir's loggers
  # for messages. See
  # https://timber.io/blog/the-ultimate-guide-to-logging-in-elixir/
  # if you are interested in this. Note we currently purge all logs
  # below Info
  require Logger
  defstruct(
    index: nil,
    pref_list: nil,
    hash_map: nil, # Store the k-value list
    range_list: nil,
    hash_tree_list: nil, # A {range:,hash_tree:} map. For each range, build a hash tree.
    message_list: nil, # Control the gossip protocol
    heartbeat_time: nil,
    heartbeat_timer: nil,
    check_time: nil,
    check_timer: nil
  )
  @spec new(non_neg_integer(),[atom()],non_neg_integer(),non_neg_integer())::%Dynamo{}
  def new(
    index,
    pref_list,
    heartbeat_time,
    check_time
    ) do
      %Dynamo{
        index: index,
        pref_list: pref_list,
        hash_map: nil,
        range_list: nil,
        hash_map: nil,
        range_list: nil,
        hash_tree_list: nil,
        message_list: nil,
        heartbeat_time: heartbeat_time,
        heartbeat_timer: nil,
        check_timer: nil,
        check_time: check_time
      }
    end

  @spec put(%Dynamo{},non_neg_integer(),{atom(),any()}):: %Dynamo{}
  def put(
    node,
    key,
    value
  ) do
    object=List.keyfind(node.hash_map,key,0)
    new_hash_map=
      if object != nil do
        new_object=%{object | value: value}
        List.keyreplace(node.hash_map,key,0,{key, new_object})
      else
        [{key, Dynamo.Object.new(value,key,[],false)} | node.hash_map]
      end
    clock_entry=List.keyfind(object.vector_clock,node.index,0)
    vector_clock=
      if clock_entry != nil do
        {_,counter}=clock_entry
       List.keyreplace(object.vector_clock,node.index,0,{node.index,counter+1})
      else
        [{node.index,1} | object.vector_clock]
      end
    %{node | hash_map: new_hash_map, vector_clock: vector_clock}
  end
  @spec get(%Dynamo{},non_neg_integer()):: non_neg_integer() | :not_exist
  def get(
    node,
    key
  ) do
    object=List.keyfind(node.hash_map,key,0)
    value=
      if object != nil do
        :not_exist
      else
        node.value
      end
  end
  @spec dynamo(%Dynamo{},any()) :: no_return()
  def dynamo(state,extra_state) do
    receive do
      {sender,%Dynamo.PutRequest{
        key: key,
        value: value,
        hash_code: hash_code,
        vector_clock: vector_clock,
        is_replica: is_replica
      }}->
        raise "Not Implemented"
      {sender,%Dynamo.PutResponse{
        key: key,
        hash_code: hash_code,
        success: success,
        is_replica: is_replica
      }}->
        raise "Not Implemented"
      {sender,%Dynamo.GetRequest{
        key: key,
        hash_tree: hash_tree,
        is_replica: is_replica
      }}->
        raise "Not Implemented"
      {sender,%Dynamo.GetResponse{
        key: key,
        value: value,
        vector_clock: vector_clock,
        is_same: is_same,
        is_replica: is_replica
      }}->
        raise "Not Implemented"
      #Gossip Potocol


    end
  end

end

defmodule Dynamo.PhysNode do
  import Emulation, only: [send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]
  alias __MODULE__
  defstruct(
    thread_name: nil, # thread name, atom
    virtual_list: nil
  )
  @spec new(atom,[atom()])::%PhysNode{}
  def new(thread_name,virtual_list) do
    %PhysNode{
      thread_name: thread_name,
      virtual_list: virtual_list
    }
  end
  @spec phys_node(%PhysNode{},any) :: no_return()
  def phys_node(state,extra_state) do
    receive do
      {sender,{:get,key}}->
        # 1 Receive message from dispatcher, transfer to the corresponding virtual node
        # 2 Return the feedback to dispatcher after gaining enough response
        raise "Not Implemented"
      {sender,{:put, key, value, hash_code}}->
        # 1 Receive message from dispatcher, transfer to the corresponding virtual node
        # 2 Return the feedback to dispatcher after gaining enough response
        raise "Not Implemented"
      {sender,{:add_node,node}}->
        # Add new virtual node to the node_list
        raise "Not Implemented"
      # {sender,{:del_node,node}}->  # No recover node, not need to implement
        # Delete existing virtual node in the node_list
        # raise "Not Implemented"
    end
  end
end
defmodule Dynamo.Dispatcher do
  import Emulation, only: [send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]
  alias __MODULE__
  defstruct(
    range_node_map: nil
  ) # [{index,node}], sorted as the index increase

  @spec new([{non_neg_integer(),atom()}]) :: %Dispatcher{}
  def new(range_node_map) do
    %Dispatcher{range_node_map: range_node_map}
  end

  @spec dispatcher(%Dispatcher{},any()) :: no_return()
  # 1 Receive the message from client, dispatch them to the corresponding physic node
  # 2 Receive the message from physic node, update the range_node_map
  def dispatcher(state,extra_state) do
    receive do
      {:message_type, value} ->
        # code
    end

  end
end
defmodule Dynamo.Client do
  import Emulation, only: [send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  alias __MODULE__
  defstruct(
    retran_time: nil,
    dispatcher: nil
    ) # List of the virtual node and its positions ,[{atom,index}]
  @spec new(non_neg_integer(),atom()) :: %Client{}
  def new(time,dispatcher) do
    %Client{retran_time: time, dispatcher: dispatcher}
  end

  @spec put(%Client{},non_neg_integer(),non_neg_integer()) :: no_return() | any()
  def put(client,key,value) do
    t = Emulation.timer(client.retran_time,:retry)
    send(client.dispatcher,{:put,key,value})
    receive do
      :retry->
        put(client,key,value)
      {_,:ok}->
        Emulation.cancel_timer(t)
        :ok
    end
  end

  @spec get(%Client{},non_neg_integer()):: no_return() | any()
  def get(client, key) do
    t = Emulation.timer(client.retran_time,:retry)
    receive do
      :retry->
        get(client, key)
      {_,{:ok,:not_exist}}->
        Emulation.cancel_timer(t)
        :not_exist
      {_,{:ok,value}}->
        Emulation.cancel_timer(t)
        value
    end
  end
end
