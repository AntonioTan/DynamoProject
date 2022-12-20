defmodule HashTest do
  use ExUnit.Case
  @spec hash_function(String.t()) :: String.t()
  def hash_function(meta_data) do
    result = MerkleTree.Crypto.hash(meta_data,:md5)
    result
  end
  @spec calculate_pos(String.t())::non_neg_integer()
  def calculate_pos(hash_code) do
    result = rem(:binary.decode_unsigned(hash_code),365)
    result
  end
  # test "Hash Tree used correct" do
  #   result= MerkleTree.Crypto.hash("tendermint", :md5)
  #   number_result = calculate_pos(result)
  #   IO.puts(number_result)
  #   assert MerkleTree.Crypto.hash("tendermint", :md5) == "bc93700bdf1d47ad28654ad93611941f"
  # end

  test "Build hash tree with own hash function" do
    mt = MerkleTree.new(["a", "b", "c","d"])
    mt2=  MerkleTree.new(["a","b","c","e"],&hash_function/1)
    mt3=MerkleTree.fast_root(["a", "b", "c","d"])

    # assert mt==mt2
    IO.inspect(mt.root.value)
    IO.inspect(mt3)
    # IO.inspect(MerkleTree.Proof.prove(mt,0))
    # IO.inspect(MerkleTree.Proof.prove(mt2,0))
  end
end
