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
import com.aerospike.client.cdt.ListPolicy;

public class ListAPIs {
	public static void appendToList(AerospikeClient client, int value) {
		Key key = new Key("test", "testList", "k1");
		client.operate(null, key, 
                  ListOperation.append(ListPolicy.Default, "myList", Value.get(value)));
	}

	public static void main(String[] args) {
		AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);
		

		Key key = new Key("test", "testList", "k1");
		WritePolicy policy = new WritePolicy();
		policy.recordExistsAction = RecordExistsAction.REPLACE;

	        client.delete(policy, key);

                appendToList(client, 1);
                appendToList(client, 5);
                appendToList(client, 3);
                appendToList(client, 1);
                appendToList(client, 8);
                appendToList(client, 4);
                appendToList(client, 3);
                appendToList(client, 9);

                System.out.println("getByIndexRange(0,8) = " + 
                  client.operate(null, key, 
                  ListOperation.getByIndexRange("myList",0,8,ListReturnType.VALUE)));

                System.out.println("getByValue(3) = " + 
                  client.operate(null, key, 
                  ListOperation.getByValue("myList",Value.get(3),ListReturnType.INDEX)));

                List<Value> compList = new ArrayList<Value>();
                compList.add(Value.get(2));
                compList.add(Value.get(3));
                compList.add(Value.get(8));

                System.out.println("getByValueList([2,3,8]) = " + 
                  client.operate(null, key, 
                  ListOperation.getByValueList("myList",compList,ListReturnType.INDEX)));

		client.close();
	}
}
