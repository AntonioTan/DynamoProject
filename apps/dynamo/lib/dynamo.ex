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
    # Store the k-value list,[{key,object}]
    hash_map: nil,
    #  Store the whole range list [{node,index}]
    view: nil,
    current_view: nil,
    # Control the gossip protocol
    message_list: nil,
    heartbeat_time: nil,
    heartbeat_timer: nil,
    check_time: nil,
    check_timer: nil,
    # W
    write_res: nil,
    # R
    read_res: nil
  )

  @spec new(
          non_neg_integer(),
          [atom()],
          [{atom(), non_neg_integer()}],
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: %Dynamo{}
  def new(
        index,
        pref_list,
        view,
        heartbeat_time,
        check_time,
        write_response,
        read_response
      ) do
    %Dynamo{
      index: index,
      pref_list: pref_list,
      #
      hash_map: [],
      view: view,
      current_view: view,
      message_list: nil,
      heartbeat_time: heartbeat_time,
      heartbeat_timer: nil,
      check_timer: nil,
      check_time: check_time,
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
    map_term = List.keyfind(node.hash_map, key, 0 , :none)
    object=
      if map_term == :none do
        :none
      else
        {_,tmp_obj}=map_term
        tmp_obj
      end

    new_object =
      if object == :none do
        object = Dynamo.Object.new(value, key, [{node.index,1}])
      else
        clock_entry=List.keyfind(object.vector_clock, node.index, 0,:none)
        new_clock=
          if clock_entry == :none do
            [{node.index, 1} | object.vector_clock]
          else
            {_, counter} = clock_entry
            List.keyreplace(object.vector_clock, node.index, 0, {node.index, counter + 1})
          end
        object=%{object | value: value, vector_clock: new_clock}
      end
    new_hash_map =
      if map_term == :none do
        List.keysort([{key,new_object} | node.hash_map], 0)
      else
        List.keyreplace(node.hash_map,key,0, {key,new_object})
      end

    %{node | hash_map: new_hash_map}
  end

  @spec get(%Dynamo{}, non_neg_integer()) :: non_neg_integer() | :not_exist
  def get(
        node,
        key
      ) do
    map_term = List.keyfind(node.hash_map, key, 0 ,:none)
    object=
      if map_term == :none do
        :none
      else
        {_,tmp_object}=map_term
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

  @spec broadcast_to_pref(%Dynamo{}, any()) :: [boolean()]
  defp broadcast_to_pref(state, message) do
    state.pref_list
    |> Enum.map(fn pid -> send(pid, message) end)
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
      {_,e}=List.first(node_map)
      {_,s}=List.last(node_map)
      {s,e}
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
          Enum.filter(hash_map, fn {index,_} -> index > endIndex and index <= startIndex end)

        first_list = Enum.filter(data, fn {index,_} -> index > startIndex end)
        second_list = Enum.filter(data, fn {index,_} -> index <= endIndex end)
        first_list ++ mid_list ++ second_list
      else
        first_list = Enum.filter(hash_map, fn {index,_} -> index <= startIndex end)
        second_list = Enum.filter(hash_map, fn {index,_} -> index > endIndex end)
        first_list ++ data ++ second_list
      end

    return_list
  end

  @spec build_tree(%Dynamo{}, {non_neg_integer(), non_neg_integer()}) :: any()
  def build_tree(state, range) do
    {startIndex, endIndex} = range

    if endIndex < startIndex do
      list1 = Enum.filter(state.hash_map, fn {key, _} -> key > endIndex end)
      list2 = Enum.filter(state.hash_map, fn {key, _} -> key < startIndex end)
      obj_list = list1 ++ list2
      mesh_list = Enum.map(obj_list, fn {key, %Dynamo.Object{value: value}} -> value end)
      mt = MerkleTree.new(mesh_list, fn x -> x end)
    else
      obj_list =
        Enum.filter(state.hash_map, fn {key, _} -> key <= endIndex and key > startIndex end)

      mesh_list = Enum.map(obj_list, fn {key, %Dynamo.Object{value: value}} -> value end)
      mt = MerkleTree.new(mesh_list, fn x -> x end)
    end
  end

  @spec syn_pref(%Dynamo{}, non_neg_integer()) :: [boolean()]
  def syn_pref(state, index) do
    {startIndex, endIndex} = find_range(state.current_view, index)
    hash_tree = build_tree(state, {startIndex, endIndex})

    broadcast_to_pref(
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
        state = put(state, key, Dynamo.Object.new(value, hash_code, vector_clock))

        broadcast_to_pref(
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

        extra_state =
          if state.write_res == 1 do
            send(sender, Dynamo.PutResponse.new(key, hash_code, true, false, client))
            extra_state
          else
            extra_state = [{:w, client, key, 1} | extra_state]
          end

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
        state = put(state, key, Dynamo.Object.new(value, hash_code, vector_clock))
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
                   fn {wr_tmp, client_tmp, key_tmp, _} ->
                     wr_tmp == :w and client_tmp == client and key_tmp == key
                   end
                 ),
                 0,
                 :none
               ) do
            :none ->
              extra_state

            item ->
              {_, _, _, vote_num} = item

              if vote_num + 1 > state.write_res do
                send(
                  client,
                  Dynamo.PutResponse.new(
                    key,
                    hash_code,
                    success,
                    false,
                    client
                  )
                )

                extra_state = [Tuple.append(Tuple.delete_at(item, 3), vote_num + 1) | extra_state]
              else
                if vote_num + 1 >= length(state.pref_list) do
                  # Start synchronize
                  syn_pref(state, key)
                  extra_state = List.delete(extra_state, item)
                else
                  extra_state = [
                    Tuple.append(Tuple.delete_at(item, 3), vote_num + 1) | extra_state
                  ]
                end
              end
          end

      {sender,
       %Dynamo.GetRequest{
         key: key,
         is_replica: false,
         client: client
       }} ->
        extra_state =
          if state.write_res == 1 do
            %Dynamo.Object{value: value, vector_clock: vector_clock} =
              Enum.at(state.hash_map, key, :not_exist)

            send(sender, Dynamo.GetResponse.new(key, value, vector_clock, false, client))
            extra_state
          else
            extra_state = [{:r, client, key, 1} | extra_state]
          end

        dynamo(state, extra_state)

      {sender,
       %Dynamo.GetRequest{
         key: key,
         is_replica: true,
         client: client
       }} ->
        %Dynamo.Object{value: value, vector_clock: vector_clock} =
          Enum.at(state.hash_map, key, :not_exist)

        send(sender, Dynamo.GetResponse.new(key, value, vector_clock, true, client))
        dynamo(state, extra_state)

      {sender,
       %Dynamo.GetResponse{
         key: key,
         value: value,
         vector_clock: vector_clock,
         is_replica: true,
         client: client
       }} ->
        extra_state =
          case Enum.at(
                 Enum.filter(
                   extra_state,
                   fn {wr_tmp, client_tmp, key_tmp, _} ->
                     wr_tmp == :r and client_tmp == client and key_tmp == key
                   end
                 ),
                 0,
                 :none
               ) do
            :none ->
              extra_state

            item ->
              {_, _, _, vote_num} = item

              if vote_num + 1 > state.write_res do
                send(
                  client,
                  Dynamo.GetResponse.new(
                    key,
                    value,
                    vector_clock,
                    false,
                    client
                  )
                )

                extra_state = [Tuple.append(Tuple.delete_at(item, 3), vote_num + 1) | extra_state]
              else
                if vote_num + 1 >= length(state.pref_list) do
                  # Start synchronize
                  syn_pref(state, key)
                  extra_state = List.delete(extra_state, item)
                else
                  extra_state = [
                    Tuple.append(Tuple.delete_at(item, 3), vote_num + 1) | extra_state
                  ]
                end
              end
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
    end
  end
end

defmodule Dynamo.Dispatcher do
  import Emulation, only: [send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  alias __MODULE__

  defstruct(node_map: nil)

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
        hash_code = hash_fun(key)
        send(node, Dynamo.PutRequest.new(key, value, hash_code, nil, false, sender))
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
        send(node, Dynamo.GetRequest.new(key, false, sender))
        dispatcher(state, extra_state)

      {sender,
       %Dynamo.GetResponse{
         key: key,
         value: value,
         vector_clock: vector_clock,
         is_replica: false,
         client: client
       }} ->
        send(client, {:ok, value})
        dispatcher(state, extra_state)

      {sender, {client, :not_exist}} ->
        send(client, :not_exist)
        dispatcher(state, extra_state)
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
        :not_exist

      {_, {:ok, value}} ->
        Emulation.cancel_timer(t)
        value
    end
  end
end
