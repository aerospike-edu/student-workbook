package com.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapWriteMode;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

public class AddToMap {
	public static void putUnordered(AerospikeClient client, int k, int v) {
		Key key = new Key("test", "s1", 1);  //Record key1 
                MapPolicy mPolicy = new MapPolicy(MapOrder.UNORDERED, MapWriteMode.UPDATE); 
		client.operate(null, key, 
                          MapOperation.put(mPolicy, "myMap", Value.get(k), Value.get(v))
                        );
	}
	public static void putKOrdered(AerospikeClient client, int k, int v) {
		Key key = new Key("test", "s1", 2);  //Record key2 
                MapPolicy mPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE); 
		client.operate(null, key, 
                          MapOperation.put(mPolicy, "myMap", Value.get(k), Value.get(v))
                        );
	}
	public static void putKVOrdered(AerospikeClient client, int k, int v) {
		Key key = new Key("test", "s1", 3);  //Record key3
                MapPolicy mPolicy = new MapPolicy(MapOrder.KEY_VALUE_ORDERED, MapWriteMode.UPDATE); 
		client.operate(null, key, 
                          MapOperation.put(mPolicy, "myMap", Value.get(k), Value.get(v))
                        );
	}

	public static void main(String[] args) {
		AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);

	        //Insert 3 records, key1 for UNORDERED, key2 for KEY_ORDERED 
                //and key3 for KEY_VALUE_ORDERED	

		Key key1 = new Key("test", "s1", 1);
		Key key2 = new Key("test", "s1", 2);
		Key key3 = new Key("test", "s1", 3);
		WritePolicy policy = new WritePolicy();
		policy.recordExistsAction = RecordExistsAction.UPDATE;
		
		client.delete(policy, key1);
		client.delete(policy, key2);
		client.delete(policy, key3);

                client.put(policy, key1, new Bin("id","key1"));
                client.put(policy, key2, new Bin("id","key2"));
                client.put(policy, key3, new Bin("id","key3"));


	       	putUnordered(client,  1,1);
	       	putUnordered(client,  7,4);
	       	putUnordered(client,  3,6);
	       	putUnordered(client,  7,1);
	       	putUnordered(client,  5,3);
	       	putUnordered(client,  6,8);


	       	putKOrdered(client,  1,1);
	       	putKOrdered(client,  7,4);
	       	putKOrdered(client,  3,6);
	       	putKOrdered(client,  7,1);
	       	putKOrdered(client,  5,3);
	       	putKOrdered(client,  6,8);


	       	putKVOrdered(client,  1,1);
	       	putKVOrdered(client,  7,4);
	       	putKVOrdered(client,  3,6);
	       	putKVOrdered(client,  7,1);
	       	putKVOrdered(client,  5,3);
	       	putKVOrdered(client,  6,8);


                System.out.println("Key1,UNORDERED, index 0 = "+ client.operate(null, key1, 
                  MapOperation.getByIndex("myMap", 0, MapReturnType.VALUE)));

                System.out.println("Key2, KEY_ORDERED, index 0 = "+ client.operate(null, key2, 
                  MapOperation.getByIndex("myMap", 0, MapReturnType.VALUE)));

                System.out.println("Key3, KEY_VALUE_ORDERED, index 0 = "+ client.operate(null, key3, 
                  MapOperation.getByIndex("myMap", 0, MapReturnType.VALUE)));

                System.out.println("Key3, KEY_VALUE_ORDERED, Rank -1 = "+ client.operate(null, key3, 
                  MapOperation.getByRank("myMap", -1, MapReturnType.VALUE)));

                System.out.println("Key2, KEY_ORDERED, size = "+ client.operate(null, key2, 
                  MapOperation.size("myMap")));
		client.close();
	}
}
