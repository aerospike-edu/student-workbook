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
key = ("test", "incTest", 'k1')
try:
  client.remove(key) #Delete record - init for this test
  print("Initialized. (Deleted old test record)")
except aerospike.exception.RecordNotFound:
  print("Initialized.")
client.increment(key, "counter1", 1)
client.increment(key, "counter2", 1)
(key,meta,bins) = client.get(key)
print("First call: Create counter1, counter2")
print(bins)

client.increment(key, "counter1", 1)
client.increment(key, "counter3", 1)
(key,meta,bins) = client.get(key)
print("Second call: Increment counter1, create counter3")
print(bins)

client.increment(key, "counter1", 1)
client.increment(key, "counter2", 1)
(key,meta,bins) = client.get(key)
print("Third call: Increment counter1 & counter2")
print(bins)
