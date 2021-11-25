# Exercise 1
#
# Episodes 1-3 were terrible.  We want to wipe their existences from the record.
# In a single operation:
# - remove the fourth item from the 'titles' bin, and return its title
# - remove the fourth last item from the 'titles' bin, and return its title
# - remove 'Attack of the Clones' from the list in 'titles' bin, and return its title
# - remove all items in 'release' with map index between 1999 and 2005 inclusive
#   and return number of items removed.


# import the module
from __future__ import print_function
import os
import pprint
import aerospike
import aerospike_helpers
from aerospike_helpers.operations import list_operations
from aerospike_helpers.operations import map_operations

pk = 'sw'

# Configure the client
config = {
  #'hosts': [ ('54.157.24.232', 3000) ]
  'hosts': [ ('127.0.0.1', 3000) ]
}

# Create a client and connect it to the cluster
def connect(config):
	try:
		client = aerospike.client(config).connect()
	except:
		import sys
		print("failed to connect to the cluster with", config['hosts'])
		sys.exit(1)
	return client


def get_data(client, user):
	key = ('test', user, pk)
	(key, metadata, record) = client.get(key)
	pp = pprint.PrettyPrinter(indent=2)
	pp.pprint(record)


# In a single operation:
# - remove the fourth item from the 'titles' bin, and return its title
# - remove the fourth last item from the 'titles' bin, and return its title
# - remove 'Attack of the Clones' from the list in 'titles' bin, and return its title
# - remove all items in 'release' with map index between 1999 and 2005 inclusive
#   and return number of items removed.
def do_exercise(client, user):
	try:
		key = ('test', user, pk)

		ops = [ aerospike_helpers.operations.list_operations.list_remove_by_index('titles', 3, aerospike.LIST_RETURN_VALUE, ctx=None),
			aerospike_helpers.operations.list_operations.list_remove_by_index('titles', -4, aerospike.LIST_RETURN_VALUE, ctx=None),
			aerospike_helpers.operations.list_operations.list_remove_by_value('titles', 'Attack of the Clones', aerospike.LIST_RETURN_VALUE, ctx=None),
			aerospike_helpers.operations.map_operations.map_remove_by_key_range('release', '1999','2006', aerospike.MAP_RETURN_COUNT, ctx=None)]

		(key,meta,results) = client.operate_ordered(key,ops)
		pp = pprint.PrettyPrinter(indent=2)
		pp.pprint(results)
	except Exception as e:
		import sys
		print("error: {0}".format(e), file=sys.stderr)


def main():
	user = 'student'
	client = connect(config)
	do_exercise(client, user)
	get_data(client, user)

	# Close the connection to the Aerospike cluster
	client.close()

main()
