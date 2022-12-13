defmodule DynamoTest do
  use ExUnit.Case
  doctest Dynamo

  test "greets the world" do
    assert Dynamo.hello() == :world
  end

  test "merkle tree usage" do
    mt = MerkleTree.new ['a', 'b', 'c', 'd']
    assert(mt.blocks == ['a', 'b', 'c', 'd'])
    assert(MerkleTree.Crypto.hash("tendermint", :sha256)=="f6c3848fc2ab9188dd2c563828019be7cee4e269f5438c19f5173f79898e9ee6")
    assert(MerkleTree.Crypto.sha256("tendermint")=="f6c3848fc2ab9188dd2c563828019be7cee4e269f5438c19f5173f79898e9ee6")
  end

end
