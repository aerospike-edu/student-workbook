package com.aerospike;

import java.util.ArrayList;
import java.util.List;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListWriteFlags;

public class UniqueOrderedList {

	public static void main(String[] args) {
		AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);
		

		Key key = new Key("test", "op", 1);		
	    String binName = "myList";	
		client.delete(null, key);
        
		List<Value> values = new ArrayList<Value>();        
		values.add(Value.get(4));        
		values.add(Value.get(0));        
		values.add(Value.get(5));        
		values.add(Value.get(9));
		values.add(Value.get(9));  //Duplicate entry
		values.add(Value.get(15));
		values.add(Value.get(11));
		values.add(Value.get(0)); //Duplicate entry
		
		
       Record rec = client.operate(null, key,
                    ListOperation.appendItems(new ListPolicy(ListOrder.ORDERED,
                    ListWriteFlags.ADD_UNIQUE|ListWriteFlags.NO_FAIL|ListWriteFlags.PARTIAL),
    				binName, values)
                    );
       System.out.println("Inserted List: [4,0,5,9,9,15,11,0] \nUnique Ordered. Read: "+ client.get(null, key));
                
		client.close();
	}
}
