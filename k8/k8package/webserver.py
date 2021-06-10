import aerospike
from flask import Flask, request
import os

NUM_RECORDS = 10
# Configure the client
config = {
  'hosts': [ ('aerocluster', 3000) ]
  # 'hosts': [ ('aerocluster-0-0.aerocluster.aerospike.svc.cluster.local', 3000) ]
  # 'hosts': [ ('54.144.78.251', 3000) ]
}

app = Flask(__name__)

# Create a client and connect it to the cluster
try:
  client = aerospike.client(config).connect('training','aerospike')
except:
  import sys
  print("failed to connect to the cluster with", config['hosts'])
  sys.exit(1)

@app.route('/', methods=['GET', 'POST'])
def hello():
    if request.method == 'POST':
        # Records are addressable via a tuple of (namespace, set, key)
        try:
            for i in range(NUM_RECORDS):
                key = ('test', 'testset', 'key'+str(i))
                val = {
                    'name': 'John Doe' + str(i),
                    'age': 32
                }
                client.put(key, val)
            return 'Success'
        except Exception as e:
            import sys
            print("error: {0}".format(e))
            return e
        client.close()
    if request.method == 'GET':
        # Read a record
        records = []
        try:
            for i in range(NUM_RECORDS):
                key = ('test', 'testset', 'key'+str(i))
                (key, metadata, record) = client.get(key)
                records.append('key'+str(i)+': ')
                records.append(record)
            return {'result': records}
        except Exception as e:
            print("Error while reading data {}".format(e))
            return e
        client.close()

if __name__ == "__main__":
    app.run(host='0.0.0.0')
