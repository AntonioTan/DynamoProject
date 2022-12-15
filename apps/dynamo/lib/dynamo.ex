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
    # Control the gossip protocol
    message_list: nil,
    failure_node_list: nil,
    heartbeat_time: nil,
    heartbeat_timer: nil
    checkout_time: nil,
    checkout_timer: nil
  )
  @spec new(non_neg_integer(),[atom()],non_neg_integer(),non_neg_integer())::%Dynamo{}
  def new(
    index,
    pref_list,
    heartbeat_time,
    checkout_time,
    ) do
      %Dynamo{
        index: index,
        pref_list: pref_list,
        hash_map: nil,
        range_list: nil,
        hash_map: nil,
        range_list: nil,
        hash_tree_list: nil,
        message_list: MapSet.new(),
        failure_node_list: MapSet.new(),
        heartbeat_time: heartbeat_time,
        heartbeat_timer: nil,
        checkout_time: checkout_time,
        checkout_timer: nil
      }
    end

  ############### HELPER FUNCTION ###########################

  # This function broadcasts the given message to every node in the preference list
  @spec broadcast_to_pref_list(%Dynamo{}, any()) :: [(boolean)]
  defp broadcast_to_pref_list(state, msg) do
    state.pref_list
      |> Enum.map(fn neighbor_node ->
        send(neighbor_node, msg)
      end)
  end


  # This function will filter out failed node from pref_list
  @spec check_pref_list_with_failed_node(%Dynamo{}, any()) :: %Dynamo{}
  defp check_pref_list_with_failed_node(state, failed_node) do
    %{state | pref_list: state.pref_list |> Enum.filter(
      fn neighbor_node -> neighbor_node!=failed_node end
      )}
  end

  # This function will add failed node to failed node list
  @spec add_failed_node(%Dynamo{}, any()) :: %Dynamo{}
  defp add_failed_node(state, failed_node) do
    %{state | failure_node_list: MapSet.put(state.failure_node_list, failed_node)}
  end


  ############### END OF HELPER FUNCTION ###########################

  ############### GOSSIP PROTOCOL ###########################
  # Save a handle to the hearbeat timer.
  @spec save_heartbeat_timer(%Dynamo{}, reference()) :: %Dynamo{}
  defp save_heartbeat_timer(state, timer) do
    %{state | heartbeat_timer: timer}
  end

  # This function should cancel the current
  # hearbeat timer, and set  a new one. You can
  # get heartbeat timeout from `state.heartbeat_timeout`.
  # You might need to call this from your code.
  @spec reset_heartbeat_timer(%Dynamo{}) :: %Dynamo{}
  defp reset_heartbeat_timer(state) do
    if state.heartbeat_timer != nil do
      cancel_timer(state.heartbeat_timer)
    end
    heartbeat_timer = timer(state.heartbeat_time, :set_heartbeat_timeout)
    state = save_heartbeat_timer(state, heartbeat_timer)
    state
  end

  # This function will send heartbeat message to every node in the preference list
  @spec send_heartbeat_msg(%Dynamo{}) :: %Dynamo{}
  defp send_heartbeat_msg(state, idx) do
    if(idx==length(state.pref_list)) {
      state
    } else {
      send(Enum.at(state.pref_list, idx), :heartbeat_msg)
      send_heartbeat_msg(state, idx+1)
    }
  end

  # This function will init message_list with preference list
  # we assume this node has received heartbeat from every node in pref_list when it is set up for the first time
  @spec init_msg_list(%Dynamo{}) :: %Dynamo{}
  defp init_msg_list(state) do
    state = %{ state | pref_list: MapSet.new(state.pref_list)}
    state
  end

  # This function will take all necessary steps to make sure the node can send heartbeat properly
  @spec setup_node_with_heartbeat(%Dynamo{}) :: %Dynamo{}
  defp setup_node_with_heartbeat(state) do
    state = reset_heartbeat_timer(state)
    state = send_heartbeat_msg(state, 0)
    state
  end

  # This function will handle heartbeat message received from other nodes
  @spec handle_heartbeat_msg(%Dynamo{}, any()) :: %Dynamo{}
  defp handle_heartbeat_msg(state, sender) do
    already_received = MapSet.member?(state.message_list, sender)
    state = if already_received do
      state
    else
      broadcast_to_pref_list(state, %Dynamo.RedirectedHeartbeatMessage{from: sender})
      %{state | message_list: MapSet.put(state.message_list, sender)}
    end
    state
  end

  # This function will check whether the node has received heartbeat message
  # sent initially from every node in the preference list
  # if not then it will gossip about the node failure message to neighbor nodes
  @spec checkout_failure(%Dynamo{}, non_neg_integer()) :: %Dynamo{}
  defp checkout_failure(state, idx) do
    if(idx==length(state.pref_list)) {
      state
    } else {
      neighbor_node = Enum.at(state.pref_list, idx)
      whether_received_heartbeat = MapSet.member?(state.message_list, neighbor_node)
      state = if whether_received_heartbeat do
        state
      else
        broadcast_to_pref_list(state, %Dynamo.NodeFailureMessage{
          failure_node: neighbor_node
        })
        state
      end
      checkout_failure(state, idx+1)
    }
  end

  # Save a handle to the checkout timer.
  @spec save_checkout_timer(%Dynamo{}, reference()) :: %Dynamo{}
  defp save_checkout_timer(state, timer) do
    %{state | checkout_timer: timer}
  end

  # This function will cancel the checkout timer and set up a new one
  @spec reset_checkout_timer(%Dynamo{}) :: %Dynamo{}
  defp reset_checkout_timer(state) do
    if state.checkout_timer != nil do
      cancel_timer(state.checkout_timer)
    end
    checkout_timer = timer(state.checkout_time, :set_checkout_timeout)
    state = save_checkout_timer(state, checkout_timer)
    state
  end

  # This function will set up the node to make sure the node checkout failure nodes correctly
  @spec setup_node_with_checkout(%Dynamo{}) :: %Dynamo{}
  defp setup_node_with_checkout(state) do
    state = init_msg_list(state)
    state = reset_checkout_timer(state)
    state
  end


  # This function handle the node failure message and gossip to other nodes
  @spec handle_node_failure_msg(%Dynamo{}, any()) :: %Dynamo{}
  defp handle_node_failure_msg(state, failed_node) do
    already_received = MapSet.member?(state.failure_node_list, failed_node)
    state = if already_received do
      state
    else
      state = check_pref_list_with_failed_node(state, failed_node)
      state = add_failed_node(state, failed_node)
      broadcast_to_pref_list(state, %Dynamo.NodeFailureMessage.new(failed_node))
      state
    end
    state
  end

  ############### END OF GOSSIP PROTOCOL ###########################

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

  @spec node(%Dynamo{}) :: no_return()
  def node(state) do
    receive do
      :set_heartbeat_timeout ->
        state = setup_node_with_heartbeat(state)
        node(state)
      :set_checkout_timeout ->
        state = checkout_failure(state, 0)
        state = reset_checkout_timer(state)
        node(state)
      {sender, :heartbeat_msg} ->
        node(handle_heartbeat_msg(state, sender))
      {sender, %Dynamo.RedirectedHeartbeatMessage{from: from_node}} ->
        node(handle_heartbeat_msg(state, from_node))
      {sender, %Dynamo.NodeFailureMessage{failure_node: failure_node}} ->
        node(handle_node_failure_msg(state, failure_node))

    end
  end

  @spec node(%Dynamo{}) :: no_return()
  def node(state) do
    receive do
      :set_heartbeat_timeout ->
        state = setup_node_with_heartbeat(state)
        node(state)
      :set_checkout_timeout ->
        state = checkout_failure(state, 0)
        state = reset_checkout_timer(state)
        node(state)
      {sender, :heartbeat_msg} ->
        node(handle_heartbeat_msg(state, sender))
      {sender, %Dynamo.RedirectedHeartbeatMessage{from: from_node}} ->
        node(handle_heartbeat_msg(state, from_node))
      {sender, %Dynamo.NodeFailureMessage{failure_node: failure_node}} ->
        node(handle_node_failure_msg(state, failure_node))

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
