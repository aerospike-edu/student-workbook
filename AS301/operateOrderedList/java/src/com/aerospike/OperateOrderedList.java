package com.aerospike;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Record;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.ListSortFlags;
import com.aerospike.client.cdt.ListWriteFlags;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

public class OperateOrderedList {

	public static void main(String[] args) {
		AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);
		

		Key key = new Key("test", "op", 1);
		WritePolicy policy = new WritePolicy();
		policy.recordExistsAction = RecordExistsAction.UPDATE;
	        String binName = "myList";	
		client.delete(policy, key);
                List<Integer> values = new ArrayList<Integer>();
                values.add(1);
                values.add(3);
                values.add(2);
                values.add(5);
                values.add(7);
                values.add(8);
                values.add(4);
                values.add(6);

                client.put(policy, key, new Bin(binName, values));
                System.out.println("Starting List: "+ client.get(null, key));
                
                Record record = client.operate(null, key,
				ListOperation.append(binName, Value.get(10)), //Returns @ 0
				ListOperation.sort(binName, ListSortFlags.DEFAULT), //No return value
				ListOperation.size(binName), //Returns @ 1
				ListOperation.getByRankRange(binName, -3,3,ListReturnType.VALUE) //Returns @ 2
				);
				
		List<?> list = record.getList(binName);
		
		long val = (Long)list.get(0);	
                System.out.println("Return val on append - list.get(0)="+ val);
		
		
		val = (Long)list.get(1);	
                System.out.println("Return val on size - list.get(1)="+ val);
		
		List<?> lval = (ArrayList)list.get(2);
                System.out.println("Return ArrayList on getByRankRange - list.get(2)="+ lval);
		

                System.out.println("After adding 9, sorting: "+ client.get(null, key));
		client.close();
	}
}
