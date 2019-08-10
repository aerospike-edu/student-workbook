package com.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Record;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

public class GetRelativeIndex {
	public static void putUnordered(AerospikeClient client, int k, int v) {
             Key key = new Key("test", "s1", 1);  //Record key1 
             MapPolicy mPolicy = new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.DEFAULT); 
	     client.operate(null, key, 
             MapOperation.put(mPolicy, "myMap", Value.get(k), Value.get(v))
             );
	}

	public static Record getRelKey(AerospikeClient client, int k, int index) {
	     Key key = new Key("test", "s1", 1);  //Record key1
             //MapPolicy mPolicy = new MapPolicy(MapOrder.DEFAULT, MapWriteFlags.DEFAULT); 
	     return client.operate(null, key, 
             MapOperation.getByKeyRelativeIndexRange("myMap", Value.get(k), index, 
             MapReturnType.KEY_VALUE));
        }

	public static Record getRelKeyCount(AerospikeClient client, int k, int index, int count) {
	     Key key = new Key("test", "s1", 1);  //Record key1
             //MapPolicy mPolicy = new MapPolicy(MapOrder.DEFAULT, MapWriteFlags.DEFAULT); 
	     return client.operate(null, key, 
             MapOperation.getByKeyRelativeIndexRange("myMap", Value.get(k), index, count, 
             MapReturnType.KEY_VALUE));
        }

	public static void main(String[] args) {
	  AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);

          //Insert record key1 for UNORDERED
	  Key key1 = new Key("test", "s1", 1);
	  WritePolicy policy = new WritePolicy();
	  policy.recordExistsAction = RecordExistsAction.UPDATE;
	  client.delete(policy, key1);

          client.put(policy, key1, new Bin("id","key1"));
          putUnordered(client,  1,1);
	  putUnordered(client,  7,4);
	  putUnordered(client,  3,6);
	  putUnordered(client,  7,1);
	  putUnordered(client,  5,3);
	  putUnordered(client,  6,8);
	  putUnordered(client,  4,3);

          for(int k=0; k<6; k++){
            System.out.println("Key1,UNORDERED, index "+k+"  = "+ client.operate(null, key1, 
                              MapOperation.getByIndex("myMap", k, MapReturnType.KEY_VALUE)));
          }

          //getRelKey
          System.out.println("Get by Index, Relative to mapkey, key=5, index=0 "+ 
                              getRelKey(client, 5, 0));
          System.out.println("Get by Index, Relative to mapkey, key=5, index=-1 "+ 
                              getRelKey(client, 5, -1));
          System.out.println("Get by Index, Relative to mapkey, key=5, index=1 "+ 
                              getRelKey(client, 5, 1));

	  //getRelKeyCount
          System.out.println("Get by Index, Relative to mapkey, limited to count, "+ 
                            "key=5, index=0, count 2 "+ getRelKeyCount(client, 5, 0,2));
          System.out.println("Get by Index, Relative to mapkey, limited to count, "+ 
                            "key=5, index=-1, count 1 "+ getRelKeyCount(client, 5, -1,1));
          System.out.println("Get by Index, Relative to mapkey, limited to count, "+ 
                            "key=5, index=1, count 1 "+ getRelKeyCount(client, 5, 1,1));

          client.close();
        }
}
