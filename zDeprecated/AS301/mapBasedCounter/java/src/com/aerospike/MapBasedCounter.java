package com.aerospike;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Record;
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

public class MapBasedCounter {
	public static void putItemsKOrdered(AerospikeClient client, Key key, Map<Value,Value> m) {
                MapPolicy mPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE); 
		client.operate(null, key, 
                          MapOperation.putItems(mPolicy, "myMap", m)
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

                client.put(policy, key1, new Bin("id","groupID1"));
                client.put(policy, key2, new Bin("id","groupID2"));
                client.put(policy, key3, new Bin("id","groupID3"));

                Map<Value, Value> m1 = new HashMap<Value, Value>();

                m1.put(Value.get("cv1"), Value.get(11));
                m1.put(Value.get("cv2"), Value.get(12));
                m1.put(Value.get("cv3"), Value.get(13));
                m1.put(Value.get("cv4"), Value.get(14));
                m1.put(Value.get("cv5"), Value.get(15));
	       	putItemsKOrdered(client, key1,m1);
	       	

                m1.put(Value.get("cv1"), Value.get(21));
                m1.put(Value.get("cv2"), Value.get(22));
                m1.put(Value.get("cv3"), Value.get(23));
                m1.put(Value.get("cv4"), Value.get(24));
                m1.put(Value.get("cv5"), Value.get(25));
                putItemsKOrdered(client, key2,m1);

                m1.put(Value.get("cv1"), Value.get(31));
                m1.put(Value.get("cv2"), Value.get(32));
                m1.put(Value.get("cv3"), Value.get(33));
                m1.put(Value.get("cv4"), Value.get(34));
                m1.put(Value.get("cv5"), Value.get(35));
	       	putItemsKOrdered(client, key3,m1);


               System.out.println("\nRecords inserted:"); 
               for(int i=0; i<5;i++){
                System.out.println("\nKey1,KEY_ORDERED, index"+i+" = "+ client.operate(null, key1, 
                  MapOperation.getByIndex("myMap", i, MapReturnType.VALUE)));
                System.out.println("Key2,KEY_ORDERED, index"+i+" = "+ client.operate(null, key2, 
                  MapOperation.getByIndex("myMap", i, MapReturnType.VALUE)));
                System.out.println("Key3,KEY_ORDERED, index"+i+" = "+ client.operate(null, key3, 
                  MapOperation.getByIndex("myMap", i, MapReturnType.VALUE)));
               }


               System.out.println("\nGet GroupID2, Read/Write Multiple Ops:"); 
               Record rec =  client.operate(null, key2,  
                  MapOperation.increment(MapPolicy.Default, "myMap", Value.get("cv1"), Value.get(1)),
                  MapOperation.increment(MapPolicy.Default, "myMap", Value.get("cv3"), Value.get(1)),
                  MapOperation.increment(MapPolicy.Default, "myMap", Value.get("cv1"), Value.get(-1)),
                  MapOperation.put(MapPolicy.Default, "myMap", Value.get("cv6"), Value.get(26)),
                  MapOperation.getByKey("myMap", Value.get("cv6"), MapReturnType.VALUE)
               );

               List<?> retList = (ArrayList)rec.getList("myMap");
               System.out.println("\nKey2, MultiOps inc cv1:21 by 1 = "+ retList.get(0));
               System.out.println("\nKey2, MultiOps inc cv3:23 by 1 = "+ retList.get(1));
               System.out.println("\nKey2, MultiOps dec cv1 (22) by 1 = "+ retList.get(2));
               System.out.println("\nKey2, MultiOps put cv6:26, map size = "+ retList.get(3));
               System.out.println("\nKey2, MultiOps get cv6 (exp:26) = "+ retList.get(4));

               client.close();
	}
}
