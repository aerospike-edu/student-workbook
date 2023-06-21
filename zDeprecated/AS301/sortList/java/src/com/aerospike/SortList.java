package com.aerospike;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.ListSortFlags;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

public class SortList {
	public static void insert(AerospikeClient client, String value) {
		Key key = new Key("test", "testList", "id1234");
		client.operate(null, key, 
                        ListOperation.insert("myList",0, Value.get(value)),
			//ListOperation.trim("myList", 0, 20)
			ListOperation.removeByIndexRange("myList", 0, 20, ListReturnType.INVERTED)
                              );
		

	}
	public static void main(String[] args) {
		AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);
		

		Key key = new Key("test", "testList", "id1234");
		List<String> values = new ArrayList<String>();
		WritePolicy policy = new WritePolicy();
		policy.recordExistsAction = RecordExistsAction.REPLACE;
		
		client.put(policy, key, new Bin("myList", values));

		for (int i = 0; i < 25; i++) {
			insert(client, "key" + (i+1));
		}
                client.operate(null, key, ListOperation.sort("myList",ListSortFlags.DEFAULT));
		client.close();
	}
}
