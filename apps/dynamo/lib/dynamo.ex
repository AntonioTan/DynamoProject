defmodule Dynamo do
  import Emulation, only: [send: 2, timer: 1, now: 0, whoami: 0, cancel_timer: 1, timer: 2]

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
    # Store the k-value list
    hash_map: nil,
    dispatcher: nil,
    view: nil,
    current_view: nil,
    dispatcher: nil,
    # A {range:,hash_tree:} map. For each range, build a hash tree.
    hash_tree_list: nil,
    # Control the gossip protocol
    message_list: nil,
    failure_node_list: nil,
    heartbeat_time: nil,
    heartbeat_timer: nil,
    checkout_time: nil,
    checkout_timer: nil,
    # W
    write_res: nil,
    # R
    read_res: nil
  )

  @spec new(
          non_neg_integer(),
          atom(),
          [atom()],
          [{atom(), non_neg_integer()}],
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: %Dynamo{}
  def new(
        index,
        dispatcher,
        pref_list,
        view,
        heartbeat_time,
        checkout_time,
        write_response,
        read_response
      ) do
    %Dynamo{
      index: index,
      dispatcher: dispatcher,
      pref_list: pref_list,
      #
      hash_map: [],
      view: view,
      current_view: view,
      message_list: MapSet.new(),
      failure_node_list: MapSet.new(),
      heartbeat_time: heartbeat_time,
      heartbeat_timer: nil,
      checkout_timer: nil,
      checkout_time: checkout_time,
      write_res: write_response,
      read_res: read_response
    }
  end

  @spec put(%Dynamo{}, non_neg_integer(), non_neg_integer()) :: %Dynamo{}
  def put(
        node,
        key,
        value
      ) do
    map_term = List.keyfind(node.hash_map, key, 0, :none)

    object =
      if map_term == :none do
        :none
      else
        {_, tmp_obj} = map_term
        tmp_obj
      end

    new_object =
      if object == :none do
        object = Dynamo.Object.new(value, key, [{node.index, 1}])
      else
        clock_entry = List.keyfind(object.vector_clock, node.index, 0, :none)

        new_clock =
          if clock_entry == :none do
            [{node.index, 1} | object.vector_clock]
          else
            {_, counter} = clock_entry
            List.keyreplace(object.vector_clock, node.index, 0, {node.index, counter + 1})
          end

        object = %{object | value: value, vector_clock: new_clock}
      end

    new_hash_map =
      if map_term == :none do
        List.keysort([{key, new_object} | node.hash_map], 0)
      else
        List.keyreplace(node.hash_map, key, 0, {key, new_object})
      end

    %{node | hash_map: new_hash_map}
  end

  ############### HELPER FUNCTION ###########################

  # This function broadcasts the given message to every node in the preference list
  @spec broadcast_to_pref_list(%Dynamo{}, any()) :: [boolean]
  defp broadcast_to_pref_list(state, msg) do
    state.pref_list
    |> Enum.map(fn {neighbor_node, _} ->
      send(neighbor_node, msg)
    end)
  end

  # # This function will filter out failed node from pref_list
  # @spec check_pref_list_with_failed_node(%Dynamo{}, any()) :: %Dynamo{}
  # defp check_pref_list_with_failed_node(state, failed_node) do
  #   %{state | pref_list: state.pref_list |> Enum.filter(
  #     fn neighbor_node -> neighbor_node != failed_node end
  #     )}
  # end

  # This function will add failed node to failed node list
  @spec add_failed_node(%Dynamo{}, any()) :: %Dynamo{}
  defp add_failed_node(state, failed_node) do
    %{state | failure_node_list: MapSet.put(state.failure_node_list, failed_node)}
  end

  defp remove_failed_node_from_pref_list(state, failed_node) do
    %{state | pref_list: state.pref_list |> Enum.filter(fn {x, _} -> x != failed_node end)}
  end

  # This function will get the name from view given index
  @spec getNameFromView(list({atom(), non_neg_integer()}), non_neg_integer()) ::
          {atom(), non_neg_integer()}
  defp getNameFromView(view, idx) do
    Enum.at(view, rem(idx, length(view)))
  end

  # This function will generate preference list for one node given N and its index in the ring
  @spec getPreferenceList(list({atom(), non_neg_integer()}), pos_integer(), non_neg_integer()) ::
          list({atom(), non_neg_integer()})
  defp getPreferenceList(view, n, start_idx) do
    Enum.to_list(1..(n - 1))
    |> Enum.map(fn x ->
      getNameFromView(view, start_idx + x)
    end)
  end

  # This function will update pref list given new current view
  @spec update_pref_list(%Dynamo{}) :: %Dynamo{}
  defp update_pref_list(state) do
    node = whoami()
    n = length(state.pref_list) + 1
    idx = Enum.find_index(state.current_view, fn {x, _} -> x == node end)

    if idx == nil do
      state
    else
      %{state | pref_list: getPreferenceList(state.current_view, n, idx)}
    end
  end

  # This function will init message_list with preference list
  # we assume this node has received heartbeat from every node in pref_list when it is set up for the first time
  @spec init_msg_list(%Dynamo{}) :: %Dynamo{}
  defp init_msg_list(state) do
    state = %{
      state
      | message_list: MapSet.new(state.pref_list |> Enum.map(fn {node, _} -> node end))
    }

    state
  end

  # This function will clear message_list and make it empty
  @spec clear_msg_list(%Dynamo{}) :: %Dynamo{}
  defp clear_msg_list(state) do
    state = %{state | message_list: MapSet.new()}
    state
  end

  # This function will set up node with timers
  @spec setup_node(%Dynamo{}) :: no_return()
  def setup_node(state) do
    state = setup_node_with_checkout(state)
    state = setup_node_with_heartbeat(state)
    dynamo(state, [])
  end

  # This function will update current view with new failure_node_list to ensure no failure node exists in current view
  @spec update_current_view(%Dynamo{}) :: %Dynamo{}
  defp update_current_view(state) do
    %{
      state
      | current_view:
          state.view |> Enum.filter(fn {x, _} -> !MapSet.member?(state.failure_node_list, x) end)
    }
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
  @spec send_heartbeat_msg(%Dynamo{}, non_neg_integer()) :: %Dynamo{}
  defp send_heartbeat_msg(state, idx) do
    if(idx >= length(state.pref_list)) do
      state
    else
      {node, _} = Enum.at(state.pref_list, idx)
      send(node, :heartbeat_msg)
      send_heartbeat_msg(state, idx + 1)
    end
  end

  # This function will take all necessary steps to make sure the node can send heartbeat properly
  @spec setup_node_with_heartbeat(%Dynamo{}) :: %Dynamo{}
  defp setup_node_with_heartbeat(state) do
    state = reset_heartbeat_timer(state)
    broadcast_to_pref_list(state, :heartbeat_msg)
    state
  end

  # This function will handle heartbeat message received from other nodes
  @spec handle_heartbeat_msg(%Dynamo{}, any()) :: %Dynamo{}
  defp handle_heartbeat_msg(state, sender) do
    already_received = MapSet.member?(state.message_list, sender)

    state =
      if already_received do
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
    if(idx >= length(state.pref_list)) do
      state
    else
      {neighbor_node, _} = Enum.at(state.pref_list, idx)
      whether_received_heartbeat = MapSet.member?(state.message_list, neighbor_node)

      state =
        if whether_received_heartbeat do
          state
        else
          broadcast_to_pref_list(state, Dynamo.NodeFailureMessage.new(neighbor_node))
          state = handle_current_view_change(state, neighbor_node)
          state
        end

      checkout_failure(state, idx + 1)
    end
  end

  # This function will change view according to failed node and change preference list given new current view
  @spec handle_current_view_change(%Dynamo{}, atom()) :: %Dynamo{}
  defp handle_current_view_change(state, failed_node) do
    state = add_failed_node(state, failed_node)
    state = update_current_view(state)
    state = update_pref_list(state)
    state = init_msg_list(state)
    state
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
  # Attention: this function should be called before setup_node_with_heartbeat since we need to init message list before sending heartbeat
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

    state =
      if already_received do
        state
      else
        broadcast_to_pref_list(state, Dynamo.NodeFailureMessage.new(failed_node))
        send(state.dispatcher, Dynamo.NodeFailureMessage.new(failed_node))
        state = handle_current_view_change(state, failed_node)
        # IO.puts("Node #{whoami()} Create failure message for #{failed_node}")
        state
      end

    state
  end

  # This function handle the checkout timeout message
  @spec handle_checkout_timeout(%Dynamo{}) :: %Dynamo{}
  defp handle_checkout_timeout(state) do
    state = checkout_failure(state, 0)
    state = reset_checkout_timer(state)
    state = clear_msg_list(state)
    state
  end

  ############### END OF GOSSIP PROTOCOL ###########################

  @spec get(%Dynamo{}, non_neg_integer()) :: non_neg_integer() | :not_exist
  def get(
        node,
        key
      ) do
    map_term = List.keyfind(node.hash_map, key, 0, :none)

    object =
      if map_term == :none do
        :none
      else
        {_, tmp_object} = map_term
        tmp_object
      end

    value =
      if object == :none do
        :not_exist
      else
        object.value
      end

    value
  end

  @spec find_range([any()], non_neg_integer()) :: {non_neg_integer(), non_neg_integer}
  def find_range(node_map, key) do
    startIndex =
      case List.last(Enum.filter(node_map, fn {_, index} -> index < key end), :none) do
        :none ->
          :none

        {_, index} ->
          index
      end

    endIndex =
      case List.first(Enum.filter(node_map, fn {_, index} -> index >= key end), :none) do
        :none ->
          :none

        {_, index} ->
          index
      end

    if startIndex == :none or endIndex == :none do
      {_, e} = List.first(node_map)
      {_, s} = List.last(node_map)
      {s, e}
    else
      {startIndex, endIndex}
    end
  end

  @spec find_data([any()], {non_neg_integer(), non_neg_integer()}) :: [any()]
  def find_data(hash_map, range) do
    {startIndex, endIndex} = range

    return_list =
      if endIndex < startIndex do
        first_list = Enum.filter(hash_map, fn {index, _} -> index > startIndex end)
        second_list = Enum.filter(hash_map, fn {index, _} -> index <= endIndex end)
        first_list ++ second_list
      else
        list =
          Enum.filter(hash_map, fn {index, _} -> index > startIndex and index <= endIndex end)

        list
      end

    return_list
  end

  @spec update_data([any()], {non_neg_integer(), non_neg_integer()}, [any()]) :: [any()]
  def update_data(hash_map, range, data) do
    {startIndex, endIndex} = range

    return_list =
      if endIndex < startIndex do
        mid_list =
          Enum.filter(hash_map, fn {index, _} -> index > endIndex and index <= startIndex end)

        first_list = Enum.filter(data, fn {index, _} -> index > startIndex end)
        second_list = Enum.filter(data, fn {index, _} -> index <= endIndex end)
        first_list ++ mid_list ++ second_list
      else
        first_list = Enum.filter(hash_map, fn {index, _} -> index <= startIndex end)
        second_list = Enum.filter(hash_map, fn {index, _} -> index > endIndex end)
        first_list ++ data ++ second_list
      end

    return_list
  end

  @spec build_tree(%Dynamo{}, {non_neg_integer(), non_neg_integer()}) :: any()
  def build_tree(state, range) do
    {startIndex, endIndex} = range

    obj_list=
    if endIndex < startIndex do
      list1 = Enum.filter(state.hash_map, fn {key, _} -> key <= endIndex end)
      list2 = Enum.filter(state.hash_map, fn {key, _} -> key > startIndex end)
      obj_list = list1 ++ list2
    else
      obj_list =
        Enum.filter(state.hash_map, fn {key, _} -> key <= endIndex and key > startIndex end)
    end
    mt=
    if Enum.empty?(obj_list) do
      mt = MerkleTree.new([-1], fn x -> x end)
    else
      mesh_list = Enum.map(obj_list, fn {key, %Dynamo.Object{value: value}} -> value end)
      mt = MerkleTree.new(mesh_list, fn x -> x end)
    end

  end

  @spec syn_pref(%Dynamo{}, non_neg_integer()) :: [boolean()]
  def syn_pref(state, index) do
    {startIndex, endIndex} = find_range(state.current_view, index)
    hash_tree = build_tree(state, {startIndex, endIndex})

    broadcast_to_pref_list(
      state,
      Dynamo.SynRequest.new(
        hash_tree,
        {startIndex, endIndex}
      )
    )
  end

  @spec dynamo(%Dynamo{}, any()) :: no_return()
  # extra_state initial to []
  def dynamo(state, extra_state) do
    receive do
      {sender,
       %Dynamo.PutRequest{
         key: key,
         value: value,
         hash_code: hash_code,
         vector_clock: vector_clock,
         is_replica: false,
         client: client
       }} ->
        state = put(state, key, value)
        if state.write_res == 1 do
          send(sender, Dynamo.PutResponse.new(key, hash_code, true, false, client))
        end
        extra_state = [{:w, client, key, true, 1} | extra_state]
        broadcast_to_pref_list(
          state,
          Dynamo.PutRequest.new(
            key,
            value,
            hash_code,
            vector_clock,
            true,
            client
          )
        )
        dynamo(state, extra_state)

      {sender,
       %Dynamo.PutRequest{
         key: key,
         value: value,
         hash_code: hash_code,
         vector_clock: vector_clock,
         is_replica: true,
         client: client
       }} ->
        state = put(state, key, value)
        send(sender, Dynamo.PutResponse.new(key, hash_code, true, true, client))
        dynamo(state, extra_state)

      {sender,
       %Dynamo.PutResponse{
         key: key,
         hash_code: hash_code,
         success: success,
         is_replica: true,
         client: client
       }} ->
        extra_state =
          case Enum.at(
                 Enum.filter(
                   extra_state,
                   fn {wr_tmp, client_tmp, key_tmp,_, _} ->
                     ((wr_tmp == :w) and (client_tmp == client) and (key_tmp == key))
                   end
                 ),
                 0,
                 :none
               ) do
            :none ->

              extra_state

            item ->
              # IO.inspect("Update extra_state #{sender}")
              # IO.inspect(extra_state)
              {_, _, _,_, vote_num} = item

              if vote_num + 1 >= state.write_res do
                # IO.puts("Send Write feedback")
                send(
                  state.dispatcher,
                  Dynamo.PutResponse.new(
                    key,
                    hash_code,
                    success,
                    false,
                    client
                  )
                )
              end
              item = Tuple.append(Tuple.delete_at(item, 4), vote_num + 1)
              extra_state =  [ item | Enum.filter(
                extra_state,
                fn {wr_tmp, client_tmp, key_tmp, _, _} ->
                  ((wr_tmp != :w) or (client_tmp != client) or (key_tmp != key))
                end
              )]
              extra_state=
              if vote_num  >= length(state.pref_list) do
                # Start synchronize
                syn_pref(state, key)
                extra_state = List.delete(extra_state, item)
              else
                extra_state
              end
          end

        dynamo(state, extra_state)

      {sender,
       %Dynamo.GetRequest{
         key: key,
         is_replica: false,
         client: client
       }} ->
        # IO.inspect("Get")
        entry = List.keyfind(state.hash_map, key, 0, :not_exist)

          if state.read_res == 1 do
            if entry != :not_exist do
              {_, %Dynamo.Object{value: value, vector_clock: vector_clock}} = entry
              send(sender, Dynamo.GetResponse.new(key, value, vector_clock, false, client))
            else
              send(sender, {client, :not_exist})
            end
          end
          extra_state =
            if entry != :not_exist do
              [{:r, client, key, true, 1} | extra_state]
            else
              [{:r, client, key, false, 1} | extra_state]
            end
          broadcast_to_pref_list(state, Dynamo.GetRequest.new(key, true, client))

        dynamo(state, extra_state)

      {sender,
       %Dynamo.GetRequest{
         key: key,
         is_replica: true,
         client: client
       }} ->
        entry = List.keyfind(state.hash_map, key, 0, :not_exist)

        if entry != :not_exist do
          {_, %Dynamo.Object{value: value, vector_clock: vector_clock}} = entry
          send(sender, Dynamo.GetResponse.new(key, value, vector_clock, true, client))
        else
          send(sender, Dynamo.GetResponse.new(key, :not_exist, [], true, client))
        end

        dynamo(state, extra_state)

      {sender,
       %Dynamo.GetResponse{
         key: key,
         value: value,
         vector_clock: vector_clock,
         is_replica: true,
         client: client
       }} ->
        local_entry = List.keyfind(state.hash_map, key, 0, :not_exist)
        is_same=
        if local_entry != :not_exist do
          {_, %Dynamo.Object{value: local_value, vector_clock: local_vector_clock}} = local_entry
          local_value == value
        else
          false
        end
        extra_state =
          case Enum.at(
                 Enum.filter(
                   extra_state,
                   fn {wr_tmp, client_tmp, key_tmp,_, _} ->
                     ((wr_tmp == :r) and (client_tmp == client) and (key_tmp == key))
                   end
                 ),
                 0,
                 :none
               ) do
            :none ->
              extra_state

            item ->
              # IO.inspect("Update read extra_state #{sender}")
              {wr_tmp, client_tmp, key_tmp,prev_same, vote_num} = item

              if vote_num + 1 >= state.read_res do
                # IO.puts("Send Read feedback")
                send(
                  state.dispatcher,
                  Dynamo.GetResponse.new(
                    key,
                    value,
                    vector_clock,
                    (prev_same and is_same),
                    client
                  )
                )
              end

              item = {wr_tmp, client_tmp, key_tmp,(prev_same and is_same), vote_num + 1}
              extra_state =  [ item | Enum.filter(
                extra_state,
                fn {wr_tmp, client_tmp, key_tmp,_, _} ->
                  ((wr_tmp != :r) or (client_tmp != client) or (key_tmp != key))
                end
              )]
              extra_state=
              if vote_num  >= length(state.pref_list) do
                # Start synchronize
                syn_pref(state, key)
                extra_state = List.delete(extra_state, item)
              else
                extra_state
              end
              extra_state
          end

        dynamo(state, extra_state)

      {sender,
       %Dynamo.SynRequest{
         hash_tree: hash_tree,
         range: range
       }} ->
        local_tree = build_tree(state, range)

        if hash_tree.root.value == local_tree.root.value do
          send(sender, Dynamo.SynResponse.new(true, range))
        else
          send(sender, Dynamo.SynResponse.new(false, range))
        end

        dynamo(state, extra_state)

      {sender,
       %Dynamo.SynResponse{
         is_succ: is_succ,
         range: range
       }} ->
        if not is_succ do
          data = find_data(state.hash_map, range)
          send(sender, Dynamo.SynTransfer.new(range, data))
        end

        dynamo(state, extra_state)

      {sender,
       %Dynamo.SynTransfer{
         range: range,
         data: data
       }} ->
        update_data(state.hash_map, range, data)
        dynamo(state, extra_state)

      # Gossip Potocol
      :set_heartbeat_timeout ->
        # IO.puts("Node #{whoami()} received heartbeat_timeout")
        state = setup_node_with_heartbeat(state)
        dynamo(state, extra_state)

      :set_checkout_timeout ->
        state = handle_checkout_timeout(state)
        dynamo(state, extra_state)

      {sender, :heartbeat_msg} ->
        dynamo(handle_heartbeat_msg(state, sender), extra_state)

      {sender, %Dynamo.RedirectedHeartbeatMessage{from: from_node}} ->
        dynamo(handle_heartbeat_msg(state, from_node), extra_state)

      {sender, %Dynamo.NodeFailureMessage{failure_node: failure_node}} ->
        # IO.puts("Node #{whoami()} received node failure msg: node #{failure_node} is dead")
        dynamo(handle_node_failure_msg(state, failure_node), extra_state)

      ## message for tests ####
      {sender, :to_dead} ->
        send(sender, :received_to_dead)
        state = %{state | heartbeat_time: 100_000}
        state = reset_heartbeat_timer(state)
        dynamo(state, extra_state)

      {sender, :get_failure_list} ->
        # IO.puts("Node #{whoami()} received get_failure_list msg")
        send(sender, Dynamo.ClientFailureNodeListMessage.new(state.failure_node_list))
        dynamo(state, extra_state)

      {sender, :get_pref_list} ->
        # IO.inspect("Pref list for node: #{whoami()}")
        # state.pref_list |> Enum.each(fn x -> IO.inspect("#{x}") end)
        # IO.inspect("End for pref list for node: #{whoami()}")
        send(sender, Dynamo.ClientPrefListMessage.new(state.pref_list))
        dynamo(state, extra_state)
    end
  end

  def dead_node(state, extra_state) do
    receive do
      _ -> dead_node(state, extra_state)
    end
  end
end

defmodule Dynamo.Dispatcher do
  import Emulation, only: [send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  alias __MODULE__

  defstruct(node_map: nil)

  ##### GOSSIP PROTOCOL #####

  # This function will delete failure node from range node map
  @spec update_range_node_map_with_failure_node(%Dispatcher{}, atom()) :: %Dispatcher{}
  defp update_range_node_map_with_failure_node(state, failure_node) do
    %{
      state
      | range_node_map:
          state.range_node_map |> Enum.filter(fn {node, idx} -> node != failure_node end)
    }
  end

  ##### END OF GOSSIP PROTOCOL ####

  # [{node,index}], sorted as the index increase
  def hash_fun(a) do
    a
  end

  def find_node(node_map, key) do
    case List.first(Enum.filter(node_map, fn {_, index} -> index >= key end), :none) do
      :none ->
        elem(node_map[0], 0)

      {node, _} ->
        node
    end
  end

  @spec new([{non_neg_integer(), atom()}]) :: %Dispatcher{}
  def new(node_map) do
    %Dispatcher{node_map: node_map}
  end

  @spec dispatcher(%Dispatcher{}, any()) :: no_return()
  # 1 Receive the message from client, dispatch them to the corresponding node
  # 2 Receive the message from physic node, update the node_map
  def dispatcher(state, extra_state) do
    receive do
      {sender, {:put, key, value}} ->
        node = find_node(state.node_map, key)
        # IO.inspect("Put #{key} on #{node}")
        hash_code = hash_fun(key)
        send(node, Dynamo.PutRequest.new(key, value, hash_code, [], false, sender))
        dispatcher(state, extra_state)

      {sender,
       %Dynamo.PutResponse{
         key: key,
         hash_code: hash_code,
         success: success,
         is_replica: false,
         client: client
       }} ->
        send(client, :ok)
        dispatcher(state, extra_state)

      {sender, {:get, key}} ->
        node = find_node(state.node_map, key)
        # IO.inspect("Get #{key} on #{node}")
        send(node, Dynamo.GetRequest.new(key, false, sender))
        dispatcher(state, extra_state)

      {sender,
       %Dynamo.GetResponse{
         key: key,
         value: value,
         vector_clock: vector_clock,
         is_replica: is_same,
         client: client
       }} ->
        # IO.inspect("Get Response")
        send(client, {:ok, value, is_same})
        dispatcher(state, extra_state)

      {sender, {client, :not_exist}} ->
        send(client, :not_exist)
        dispatcher(state, extra_state)

      ####### GOSSIP PROTOCOL ################
      {sender, %Dynamo.NodeFailureMessage{failure_node: failure_node}} ->
        state = update_range_node_map_with_failure_node(state, failure_node)
        dispatcher(state, extra_state)

      ### Deal CLIENT REQUEST FOR TEST USE ###
      {sender, :getRangeNodeMap} ->
        send(sender, Dynamo.ClientRangeNodeMapMessage.new(state.range_node_map))
        dispatcher(state, extra_state)
        ###### END OF GOSSIP PROTOCOL #######
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
  )

  # List of the virtual node and its positions ,[{atom,index}]
  @spec new(non_neg_integer(), atom()) :: %Client{}
  def new(time, dispatcher) do
    %Client{retran_time: time, dispatcher: dispatcher}
  end

  @spec put(%Client{}, non_neg_integer(), non_neg_integer()) :: no_return() | any()
  def put(client, key, value) do
    t = Emulation.timer(client.retran_time, :retry)
    send(client.dispatcher, {:put, key, value})

    receive do
      :retry ->
        put(client, key, value)

      {_, :ok} ->
        Emulation.cancel_timer(t)
        :ok
    end
  end

  @spec get(%Client{}, non_neg_integer()) :: no_return() | any()
  def get(client, key) do
    t = Emulation.timer(client.retran_time, :retry)
    send(client.dispatcher, {:get, key})

    receive do
      :retry ->
        get(client, key)

      {_, :not_exist} ->
        Emulation.cancel_timer(t)
        {:not_exist, false}

      {_, {:ok, value, is_same}} ->
        Emulation.cancel_timer(t)
        {value, is_same}
    end
  end
end
