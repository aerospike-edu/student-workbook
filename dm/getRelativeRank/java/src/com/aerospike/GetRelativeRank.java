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

public class GetRelativeRank {
	public static void putUnordered(AerospikeClient client, int k, int v) {
             Key key = new Key("test", "s1", 1);  //Record key1 
             MapPolicy mPolicy = new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.DEFAULT); 
	     client.operate(null, key, 
             MapOperation.put(mPolicy, "myMap", Value.get(k), Value.get(v))
             );
	}
	
        public static Record getRelValue(AerospikeClient client, int value, int rank) {
	     Key key = new Key("test", "s1", 1);  //Record key1
             //MapPolicy mPolicy = new MapPolicy(MapOrder.DEFAULT, MapWriteFlags.DEFAULT); 
	     return client.operate(null, key, 
             MapOperation.getByValueRelativeRankRange("myMap", Value.get(value), rank, 
             MapReturnType.KEY_VALUE));
        }

	public static Record getRelValueCount(AerospikeClient client, int value, int rank, int count) {
	     Key key = new Key("test", "s1", 1);  //Record key1
             //MapPolicy mPolicy = new MapPolicy(MapOrder.DEFAULT, MapWriteFlags.DEFAULT); 
	     return client.operate(null, key, 
             MapOperation.getByValueRelativeRankRange("myMap", Value.get(value), rank, count, 
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
            System.out.println("Key1,UNORDERED, rank "+k+"  = "+ client.operate(null, key1, 
                              MapOperation.getByRank("myMap", k, MapReturnType.KEY_VALUE)));
          }

          //getRelValue
          System.out.println("Get by Rank, Relative to mapValue, value=1, rank=0 "+ 
                            getRelValue(client, 1, 0));
          System.out.println("Get by Rank, Relative to mapValue, value=3, rank=-1 "+ 
                            getRelValue(client, 3, -1));
          System.out.println("Get by Rank, Relative to mapValue, value=3, rank=1 "+ 
                            getRelValue(client, 3, 1));

          //getRelValueCount
          System.out.println("Get by Rank, Relative to mapValue, limit by count, value=1, rank=0, count=1"+ 
                            getRelValueCount(client, 1, 0, 1));
          System.out.println("Get by Rank, Relative to mapValue, limit by count, value=3, rank=-1, count=3"+ 
                            getRelValueCount(client, 3, -1, 3));
          System.out.println("Get by Rank, Relative to mapValue, limit by count, value=3, rank=1, count=3"+ 
                            getRelValueCount(client, 3, 1, 3));

          client.close();
        }
}
