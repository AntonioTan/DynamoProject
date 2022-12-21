# DynamoProject
Our project implements a simplifed version of Dynamo which is a weakly consistent key-value store.

____
There are two tests in the project. 

The dynamo_test.exs is used to test the functionality of get&put operations. 

The gossip_test.exs is used to test the functionality of gossip-based protocol

___
To run these two tests, run the following commands from the project root directory.

mix test ./apps/dynamo/test/dynamo_test.exs

mix test ./apps/dynamo/test/gossip_test.exs

When running the test for dynamo_test, you may need to set up the timeout for the unit test: "Data for Plots".

