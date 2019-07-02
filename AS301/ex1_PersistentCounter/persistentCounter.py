#! /usr/bin/python
print("\n")
print("Example 1: Group of persistent counters")
print("In each group, counters are stored in a Map type bin")
print("Multiple counters comprise the map as key-value pairs (eg 'cv2':22)")
print("************************************************************************")

import aerospike

# Open connection to the server
config = { 'hosts': [ ("localhost", 3000), ] }
client = aerospike.client(config).connect()

#Set map policy
mp={'map_order':aerospike.MAP_KEY_ORDERED}

# Insert the records
key = ("test", "persistentCounter", 'groupID1')
client.map_put_items(key, map_policy=mp,  bin="counters", items={'cv1':11, 'cv2':12, 'cv3':13, 'cv4':14, 'cv5':15} )

key = ("test", "persistentCounter", 'groupID2')
client.map_put_items(key, map_policy=mp,  bin="counters", items={'cv1':21, 'cv2':22, 'cv3':23, 'cv4':24, 'cv5':25} )

key = ("test", "persistentCounter", 'groupID3')
client.map_put_items(key, map_policy=mp,  bin="counters", items={'cv1':31, 'cv2':32, 'cv3':33, 'cv4':34, 'cv5':35} )
print("Records inserted:")
print("groupID1: cv1:11, cv2:12, cv3:13, cv4:15, cv5:15")
print("groupID2: cv1:21, cv2:22, cv3:23, cv4:25, cv5:25")
print("groupID3: cv1:31, cv2:32, cv3:33, cv4:35, cv5:35")

print("Get first 3 indexes [Specify range:0-3] of groupID2")
key = ("test", "persistentCounter", 'groupID2')
ret_val = client.map_get_by_index_range(key, "counters", 0, 3, aerospike.MAP_RETURN_VALUE)
print(ret_val)

print("Increment value of counter cv3 of groupID2 by 1")
key = ("test", "persistentCounter", 'groupID2')
client.map_increment(key, map_policy=mp,  bin="counters", map_key='cv3', incr=1)

print("Read cv3 of groupID2")
key = ("test", "persistentCounter", 'groupID2')
ret_val=client.map_get_by_key(key, bin="counters", map_key='cv3', return_type=aerospike.MAP_RETURN_VALUE)
print(ret_val)

print("Increment and read back multiple counters using operate in groupID2")
key = ("test", "persistentCounter", 'groupID2')
ops = [ {"op":aerospike.OP_MAP_INCREMENT, "bin":"counters", "key":'cv1',"val":1},
{"op":aerospike.OP_MAP_INCREMENT, "bin":"counters", "key":'cv2',"val":1},
{"op":aerospike.OP_MAP_GET_BY_KEY, "bin":"counters", "key":'cv1',"return_type":aerospike.MAP_RETURN_KEY_VALUE},
{"op":aerospike.OP_MAP_GET_BY_KEY, "bin":"counters", "key":'cv2',"return_type":aerospike.MAP_RETURN_KEY_VALUE},
{"op":aerospike.OP_MAP_GET_BY_KEY, "bin":"counters", "key":'cv3',"return_type":aerospike.MAP_RETURN_KEY_VALUE}]

(key,meta, bins) = client.operate(key,ops)
print("operate() returns output from last operation only. [cv1++, cv2++, read cv1, cv2, cv3]")
print(bins)

(key,meta, bins) = client.operate_ordered(key,ops)
print("operate_ordered() returns output from each OPERATION. [cv1++, cv2++, read cv1, cv2, cv3]")
print(bins)
