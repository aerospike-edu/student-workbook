package com.aerospike;

import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Record;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.Operation;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapWriteFlags;

public class MapWriteFlagsTest {

	public static void main(String[] args) {
		AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);
		Key key1 = new Key("test", "s1", 1);
		client.delete(null, key1);

                Bin mybin1 = new Bin("mybin1", 1);
                Bin mybin2 = new Bin("mybin2", "abc");

                Map<Value, Value> m1 = new HashMap<Value, Value>();
                m1.put(Value.get("k1"), Value.get(1));
                m1.put(Value.get("k2"), Value.get(2));
                m1.put(Value.get("k3"), Value.get(3));
                m1.put(Value.get("k4"), Value.get(4));
                m1.put(Value.get("k5"), Value.get(5));

                Map<Value, Value> m2 = new HashMap<Value, Value>();
                m2.put(Value.get("k6"), Value.get(6));
                m2.put(Value.get("k7"), Value.get(7));
                m2.put(Value.get("k8"), Value.get(8));
                m2.put(Value.get("k9"), Value.get(9));
                m2.put(Value.get("k10"), Value.get(10));

                MapPolicy mPolicy = new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.DEFAULT); 

                MapPolicy mPolicy1 = new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.CREATE_ONLY); 

                MapPolicy mPolicy2 = new MapPolicy(MapOrder.UNORDERED, 
                                         MapWriteFlags.CREATE_ONLY|MapWriteFlags.NO_FAIL); 
                MapPolicy mPolicy3 = new MapPolicy(MapOrder.UNORDERED, 
                                         MapWriteFlags.CREATE_ONLY|MapWriteFlags.NO_FAIL|MapWriteFlags.PARTIAL); 

                Record rec =  client.operate(null, key1,
                  Operation.put(mybin1),  //1
                  MapOperation.putItems(MapPolicy.Default, "myMapBin1", m1), //2
                  Operation.put(mybin2),  //3
                  MapOperation.put(mPolicy2, "myMapBin1", Value.get("k1"), Value.get(11)), //4
                  MapOperation.put(mPolicy2, "myMapBin1", Value.get("k6"), Value.get(16)), //5
                  MapOperation.putItems(mPolicy3, "myMapBin1", m2) //6
                );

               client.close();
	}
}
