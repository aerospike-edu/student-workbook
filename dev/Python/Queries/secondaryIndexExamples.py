import aerospike
from aerospike import predicates as p

def print_result((key, metadata, record)):
   print(record)

config = { 'hosts': [ ("localhost", 3000), ] }
client = aerospike.client(config).connect()

# List Index Example
client.index_list_create("test", "demo", "nums", aerospike.INDEX_NUMERIC, "nums-idx", {})

# Insert the records
key = ("test", "demo", 'kl1')
client.put(key, {'name': "List:1,2,3", 'nums': [1, 2, 3] } )

key = ("test", "demo", 'kl2')
client.put(key, {'name': "List:5,7,11,11", 'nums': [5, 7, 11, 11] } )

key = ("test", "demo", 'kl3')
client.put(key, {'name': "List:5,7", 'nums': [5, 7] } )

key = ("test", "demo", 'kl4')
client.put(key, {'name': "List:5,7,11", 'nums': [5, 7, 11] } )

# Query for value 11, will return one record
query = client.query("test", "demo")
query.where(p.contains("nums", aerospike.INDEX_TYPE_LIST, 11))
print("List Index")
query.foreach( print_result )

# Map Index Example - you can create indexes on Map Keys or Map Values

# Create index on Map Values.
client.index_map_values_create("test", "demo", "mymap", aerospike.INDEX_NUMERIC, "map-val-idx", {})
client.index_map_keys_create("test", "demo", "mymap", aerospike.INDEX_STRING, "map-key-idx", {})

# Insert the records
key = ("test", "demo", 'km1')
client.put(key, {'name': "Map:1,2,3", "mymap":{"a":1, "b":2, "c":3} })

key = ("test", "demo", 'km2')
client.put(key, {'name': "Map:5,7,11,11", "mymap":{"a":5, "b":7, "c":11, "d":11} })

key = ("test", "demo", 'km3')
client.put(key, {'name': "Map:5,7",  "mymap":{"a":5, "b":7} })

key = ("test", "demo", 'km4')
client.put(key, {'name': "Map:5,7,11",  "mymap":{"a":5, "b":7, "c":11} })

key = ("test", "demo", 'km5')
client.put(key, {'name': "Map:5,7,10,12", "mymap":{"a":5, "b":7, "c":10, "d":12} })

# Query for value 11, will return one record
query = client.query("test", "demo")
query.where(p.contains("mymap", aerospike.INDEX_TYPE_MAPVALUES, 11))
print("Map Value Index")
query.foreach( print_result )

# Query for value 11, will return one record
query = client.query("test", "demo")
query.where(p.contains("mymap", aerospike.INDEX_TYPE_MAPKEYS, "d"))
print("Map Key Index")
query.foreach( print_result )

