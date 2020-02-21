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
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.cdt.ListWriteFlags;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

public class NestedCDTs {

	public static void main(String[] args) {
		AerospikeClient client = new AerospikeClient("172.28.128.3", 3000); //Update IP Address 

	        //Insert a record with a map data type

		Key key1 = new Key("test", "s1", 1);
		WritePolicy wPolicy = new WritePolicy();
		wPolicy.recordExistsAction = RecordExistsAction.UPDATE;
		
		client.delete(wPolicy, key1);

                //Step 1
                Map<Value, Value> m1 = new HashMap<Value, Value>();
                m1.put(Value.get("l1k1"), Value.get(11));
                m1.put(Value.get("l1k2"), Value.get(12));
                m1.put(Value.get("l1k3"), Value.get(13));

                MapPolicy mPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.DEFAULT); 
		client.operate(wPolicy, key1, MapOperation.putItems(mPolicy, "myMap", m1));
	       	


               System.out.println("\nRecord inserted:"); 
               for(int i=0; i<3;i++){
                System.out.println("\nKey1,KEY_ORDERED, getByIndex("+i+") = "+ client.operate(null, key1, 
                  MapOperation.getByIndex("myMap", i, MapReturnType.VALUE)));
               }

               //Step 2: Insert nested map at l1k1:11 in key1
               
                Map<Value, Value> m2 = new HashMap<Value, Value>();
                m2.put(Value.get("l2k1"), Value.get(0));
		client.operate(wPolicy, key1, 
                          MapOperation.put(mPolicy, "myMap", Value.get("l1k1"), Value.get(m2))
                          );

                // Step 3: Reassign m2 
                m2.put(Value.get("l2k1"), Value.get(21));
                m2.put(Value.get("l2k2"), Value.get(22));
                m2.put(Value.get("l2k3"), Value.get(23));

		client.operate(wPolicy, key1, 
                          MapOperation.putItems(mPolicy, "myMap", m2, CTX.mapKey(Value.get("l1k1")) )
                          );

                //Step 4: Add another nesting level
                Map<Value, Value> m3 = new HashMap<Value, Value>();
                m3.put(Value.get("l3k1"), Value.get(0));
		client.operate(wPolicy, key1, 
                          MapOperation.put(mPolicy, "myMap", Value.get("l2k1"), Value.get(m3), CTX.mapKey(Value.get("l1k1")) )
                          );

                //Step 5: Edit second nesting level entry

                // Reassign m3 
                m3.put(Value.get("l3k1"), Value.get(31));
                m3.put(Value.get("l3k2"), Value.get(32));
                m3.put(Value.get("l3k3"), Value.get(33));
		client.operate(wPolicy, key1, 
                               MapOperation.putItems(
                               mPolicy, "myMap", m3, 
                               CTX.mapKey(Value.get("l1k1")), 
                               CTX.mapKey(Value.get("l2k1")) 
                               )
                              );

               //Step 6: Edit 2nd level nested value l3k3:33 to l3k3:99
              
		client.operate(wPolicy, key1, 
                               MapOperation.put(
                               mPolicy, "myMap", Value.get("l3k3"), Value.get(99),
                               CTX.mapKey(Value.get("l1k1")), 
                               CTX.mapKey(Value.get("l2k1")) 
                               )
                              );
 
               //Step 7: Edit 2nd level nested value l3k3:99 to list type

                List<Value> l1 = new ArrayList<Value>();        
		l1.add(Value.get(0));        

		client.operate(wPolicy, key1, 
                               MapOperation.put(
                               mPolicy, "myMap", Value.get("l3k3"), Value.get(l1),
                               CTX.mapKey(Value.get("l1k1")), 
                               CTX.mapKey(Value.get("l2k1")) 
                               )
                              );



               //Step 8: Append items directly to nested list at l3k3
             
		l1.add(Value.get(4));        
		l1.add(Value.get(1));        
		l1.add(Value.get(4));        

                ListPolicy lPolicy = new ListPolicy(ListOrder.ORDERED, 
                                     ListWriteFlags.ADD_UNIQUE|ListWriteFlags.NO_FAIL|ListWriteFlags.PARTIAL);
		client.operate(wPolicy, key1, 
                               ListOperation.appendItems(
                               lPolicy, "myMap", l1,
                               CTX.mapKey(Value.get("l1k1")), 
                               CTX.mapKey(Value.get("l2k1")), 
                               CTX.mapKey(Value.get("l3k3")) 
                               )
                              );
 

/*
*/

               client.close();
	}
}
