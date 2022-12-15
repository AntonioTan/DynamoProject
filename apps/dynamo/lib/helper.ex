defmodule Dynamo.Object do
  alias __MODULE__
  defstruct(
    value: nil,
    hash_code: nil,
    vector_clock: nil,
    is_replica: nil
  )
  def new(
    value,
    hash_code,
    vector_clock,
    is_replica
  ) do
    %Object{
      value: value,
      hash_code: hash_code,
      vector_clock: vector_clock,
      is_replica: is_replica
    }
  end
end

defmodule Dynamo.PutRequest do
  # Dispatcher to coordinator(is_replica=false)
  # Coordinator to pref_list node (is_replica=true)
  alias __MODULE__
  defstruct(
    key: nil,
    value: nil,
    hash_code: nil,
    vector_clock: nil,
    is_replica: nil
  )
  @spec new(non_neg_integer(),
  non_neg_integer(),
  non_neg_integer(),
  [{atom(),non_neg_integer()}],
  boolean()
  )::%PutRequest{}
  def new(key,value,hash_code,vector_clock,is_replica) do
    %PutRequest{
      key: key,
      value: value,
      hash_code: hash_code,
      vector_clock: vector_clock,
      is_replica: is_replica
    }
  end
end

defmodule Dynamo.PutResponse do
  # Coordinator to Dispatcher(is_replica=false)
  # pref_list node to coordinator (is_replica=true)
  alias __MODULE__
  defstruct(
    key: nil,
    hash_code: nil,
    success: nil,
    is_replica: nil
  )
  @spec new(
  non_neg_integer(),
  non_neg_integer(),
  boolean(),
  boolean()
  )::%PutResponse{}
  def new(key,hash_code,success,is_replica) do
    %PutResponse{
      key: key,
      hash_code: hash_code,
      success: success,
      is_replica: is_replica
    }
  end
end

defmodule Dynamo.GetRequest do
  # Dispatcher to coordinator(is_replica=false,hash_tree=nil)
  # Coordinator to pref_list node (is_replica=true,hash_tree !=nil)
  alias __MODULE__
  defstruct(
    key: nil,
    hash_tree: nil,
    is_replica: nil
  )

  @spec new(non_neg_integer(),any(),boolean()) :: %GetRequest{}
  def new(key,hash_tree,is_replica) do
    %GetRequest{
      key: key,
      hash_tree: hash_tree,
      is_replica: is_replica
    }
  end
end

defmodule Dynamo.GetResponse do
  alias Dynamo.GetResponse
  # Coordinator to Dispatcher(is_replica=false)
  # pref_list node to coordinator (is_replica=true)
  alias __MODULE__
  defstruct(
    key: nil,
    value: nil,
    vector_clock: nil,
    is_same: nil,
    is_replica: nil
  )
  @spec new(non_neg_integer(),non_neg_integer(),any(),boolean(),boolean()) :: %GetResponse{}
  def new(key,value,vector_clock,is_same,is_replica) do
    %GetResponse{
      key: key,
      value: value,
      vector_clock: vector_clock,
      is_same: is_same,
      is_replica: is_replica
    }
  end
end

# This is the struct for heartbeat message received from other nodes and this should only be used to redirect
defmodule Dynamo.RedirectedHeartbeatMessage do
  alias Dynamo.RedirectedHeartbeatMessage
  defstruct(
    from: nil
  )
  def new(from) do
    %RedirectedHeartbeatMessage {
      from: from
    }
  end
end

# This is the struct used to gossip about the failure of one node
defmodule Dynamo.NodeFailureMessage do
  alias Dynamo.NodeFailureMessage
  defstruct(
    failure_node: nil
  )
  def new(failure_node) do
    %NodeFailureMessage{
      failure_node: failure_node
    }
  end
end
