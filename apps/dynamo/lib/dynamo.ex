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
    phys_addr: nil, # physical node name
    pref_list: nil,
    hash_map: nil, # Store the k-value list
    range_list: nil,
    hash_tree_list: nil, # A {range:,hash_tree:} map. For each range, build a hash tree.
    message_list: nil # Control the gossip protocol
  )



end
