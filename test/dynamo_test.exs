defmodule DynamoTest do
  use ExUnit.Case
  doctest Dynamo

  test "greets the world" do
    assert Dynamo.hello() == :world
  end
end
