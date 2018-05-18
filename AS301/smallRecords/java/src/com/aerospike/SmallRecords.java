package com.aerospike;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

public class SmallRecords {
	public static void largeRecord(AerospikeClient client, String keyString) {
		Key key = new Key("test", "testList", "id1234");
		client.operate(null, key, ListOperation.insert("myList",0, Value.get(keyString)),
				ListOperation.trim("myList", 0, 20));
		

	}
	public static void main(String[] args) {
		AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);
	    int  maxRecords = 25;	
		for (int i = 0; i < maxRecords; i++) {
		  Key key = new Key("test", "smallRecordsSet", "id"+i);
		  client.put(null, key, new Bin("id", "id"+i), new Bin("name", "name"+i), new Bin ("ver", i));
		}
		client.close();

	}
}
