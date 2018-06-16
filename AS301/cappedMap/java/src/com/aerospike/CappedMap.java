package com.aerospike;

import java.lang.String;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.cdt.MapPolicy;

public class CappedMap {
	public static int insert(AerospikeClient client, int i) {
		Key key = new Key("test", "testMap", "user1");
	        MapPolicy mPolicy = new MapPolicy();	
                int retVal=0;
                try {
		        client.operate(null, key, 
                        MapOperation.put(mPolicy, "myMap", Value.get(i), 
                        Value.get("A quick brown fox jumps right over a lazy dog") ));
                 } 
                 catch (AerospikeException e) {
                   //System.out.println("Error Code: "+e.getResultCode());
                   if(e.getResultCode() == 13){
		        client.operate(null, key, 
                        MapOperation.removeByIndex("myMap", 0, retVal));  
                        --i; //Insert again
                   }
	         }	
                 return i;
	}
	public static void main(String[] args) {
		AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);
                int retVal = 0;
		for (int i = 1; i < 30; i++) {
                  i = insert(client, i);
		}
		client.close();
	}
}
