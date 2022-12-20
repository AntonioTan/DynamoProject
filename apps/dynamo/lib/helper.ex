defmodule Dynamo.Object do
  alias __MODULE__
  defstruct(
    value: nil,
    hash_code: nil,
    vector_clock: nil
  )
  def new(
    value,
    hash_code,
    vector_clock
  ) do
    %Object{
      value: value,
      hash_code: hash_code,
      vector_clock: vector_clock
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
    is_replica: nil,
    client: nil
  )
  @spec new(
  non_neg_integer(),
  non_neg_integer(),
  non_neg_integer(),
  [{atom(),non_neg_integer()}],
  boolean(),
  atom()
  )::%PutRequest{}
  def new(key,value,hash_code,vector_clock,is_replica,client) do
    %PutRequest{
      key: key,
      value: value,
      hash_code: hash_code,
      vector_clock: vector_clock,
      is_replica: is_replica,
      client: client
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
    is_replica: nil,
    client: nil
  )
  @spec new(
  non_neg_integer(),
  non_neg_integer(),
  boolean(),
  boolean(),
  atom()
  )::%PutResponse{}
  def new(key,hash_code,success,is_replica,client) do
    %PutResponse{
      key: key,
      hash_code: hash_code,
      success: success,
      is_replica: is_replica,
      client: client
    }
  end
end

defmodule Dynamo.GetRequest do
  # Dispatcher to coordinator(is_replica=false,hash_tree=nil)
  # Coordinator to pref_list node (is_replica=true,hash_tree !=nil)
  alias __MODULE__
  defstruct(
    key: nil,
    is_replica: nil,
    client: nil
  )

  @spec new(non_neg_integer(),boolean(),atom()) :: %GetRequest{}
  def new(key,is_replica,client) do
    %GetRequest{
      key: key,
      is_replica: is_replica,
      client: client
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
    is_replica: nil,
    client: nil
  )
  @spec new(non_neg_integer(),non_neg_integer(),any(),boolean(),atom()) :: %GetResponse{}
  def new(key,value,vector_clock,is_replica,client) do
    %GetResponse{
      key: key,
      value: value,
      vector_clock: vector_clock,
      is_replica: is_replica,
      client: client
    }
  end
end

defmodule Dynamo.SynRequest do
  alias __MODULE__
  defstruct(
    hash_tree: nil,
    range: nil
  )

  def new(hash_tree,range) do
    %SynRequest{
      hash_tree: hash_tree,
      range: range
    }
  end
end

defmodule Dynamo.SynResponse do
  alias __MODULE__
  defstruct(
    is_succ: nil,
    range: nil
  )
  def new(is_succ,range) do
    %SynResponse{
      is_succ: is_succ,
      range: range
    }
  end
end

defmodule Dynamo.SynTransfer do
  alias __MODULE__
  defstruct(
    range: nil,
    data: nil
  )
  def new(range,data) do
    %SynTransfer{
      range: range,
      data: data
    }
  end
end
