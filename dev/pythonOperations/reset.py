# Use this to reset your record
# Define your user first


# import the module
from __future__ import print_function
import os
import pprint
import aerospike



##########################################################
# Set your user by exporting the NAME environment variable
# eg.  $ export NAME=andre
users = [ 'dave', 'rahul', 'john', 'andre', 'piyush' ]
user = os.environ.get('NAME', 'unknown')
##########################################################

pk = 'sw'


# Configure the client
config = {
  'hosts': [ ('127.0.0.1', 3000) ]
}



# Create a client and connect it to the cluster
def connect(config):
	try:
		client = aerospike.client(config).connect()
	except:
		import sys
		print('failed to connect to the cluster with', config['hosts'])
		sys.exit(1)
	return client


def _make_data():
	data = {
		'franchise': 'Star Wars',
		'titles': [
			'A New Hope',
			'The Empire Strikes Back',
			'Return of the Jedi',
			'The Phantom Menace',
			'Attack of the Clones',
			'Revenge of the Sith',
			'The Force Awakens',
			'The Last Jedi',
			'The Rise of Skywalker',
		],
		'release': {
			'1977-05-25': [4, 'IV',		'A New Hope',				{ 'budget':11, 'box':775.8 } ],
			'1980-05-21': [5, 'V', 		'The Empire Strikes Back',	{ 'budget':30.5, 'box':538 } ],
			'1983-05-25': [6, 'VI',		'Return of the Jedi',		{ 'budget':32.5, 'box':475.1 } ],
			'1999-05-19': [1, 'I',		'The Phantom Menace',		{ 'budget':115, 'box':1027 } ],
			'2002-05-16': [2, 'II',		'Attack of the Clones',		{ 'budget':115, 'box':653.8 } ],
			'2005-05-19': [3, 'III',	'Revenge of the Sith',		{ 'budget':113, 'box':868.4 } ],
			'2015-12-18': [7, 'VII',	'The Force Awakens',		{ 'budget':259, 'box':2069 } ],
			'2017-12-15': [8, 'VIII',	'The Last Jedi',			{ 'budget':200, 'box':1333 } ],
			'2019-12-20': [9, 'IX',		'The Rise of Skywalker',	{ 'budget':275, 'box':1078 } ],
		}
	}
	return data


def reset(client, user):
	try:
		key = ('test', user, pk)

		client.put(key, _make_data(), policy={'exists': aerospike.POLICY_EXISTS_CREATE_OR_REPLACE})

		# Set map to be ordered
		map_policy={'map_order':aerospike.MAP_KEY_VALUE_ORDERED}
		client.map_set_policy(key, 'release', map_policy)
	except Exception as e:
		import sys
		print("error: {0}".format(e), file=sys.stderr)


def get_data(client, user):
	key = ('test', user, pk)
	(key, metadata, record) = client.get(key)
	pp = pprint.PrettyPrinter(indent=2)
	pp.pprint(record)


def main():
	#if user not in users:
	#	import sys
	#	print('NAME="%s" - please export NAME=<name>!' % user)
	#	sys.exit(1)

	user='student'

	client = connect(config)

	reset(client, user)

	get_data(client, user)

	# Close the connection to the Aerospike cluster
	client.close()


main()
