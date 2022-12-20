defmodule DynamoTest do
  use ExUnit.Case
  import Emulation, only: [spawn: 2, send: 2]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  test "Get and Put function" do

  end

  test "Range Function" do

  end

  test "Set up nodes and check whether nodes input correctly synchronize" do

  end

  test "Data for Plots" do

  end
end
