#! /usr/bin/python
print("\n")
print("Example 2: Increment or Create a counter")
print("If the counter does not exist, ")
print("create it with intial value 1 otherwise increment it by 1")
print("************************************************************************")

import aerospike

# Open connection to the server
config = { 'hosts': [ ("localhost", 3000), ] }
client = aerospike.client(config).connect()

#Insert the record containing the counter, create if it does not exist
key = ("test", "incrementCounter", 'k1')
client.increment(key, "counter", 1)
(key,meta,bins) = client.get(key)
print("First call")
print(bins)

key = ("test", "incrementCounter", 'k1')
client.increment(key, "counter", 1)
(key,meta,bins) = client.get(key)
print("Second call")
print(bins)

key = ("test", "incrementCounter", 'k1')
client.increment(key, "counter", 1)
(key,meta,bins) = client.get(key)
print("Third call")
print(bins)
