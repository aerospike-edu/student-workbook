package com.aerospike;

import java.lang.String;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapPolicy;

public class CappedMap {
	public static int insert(AerospikeClient client, int i) {
		Key key = new Key("test", "testMap", "user1");
	        MapPolicy mPolicy = new MapPolicy();	
                int retVal=0;
                try {
		        client.operate(null, key, 
                        //MapOperation.removeByIndexRange("myMap",-10,10,MapReturnType.INVERTED), 
                        // INVERTED introduced in server version 3.16.0.1
                        MapOperation.put(mPolicy, "myMap", Value.get(i), 
                        Value.get("A quick brown fox jumps right over a lazy dog") ));
                 } 
                 catch (AerospikeException e) {
                   System.out.println("Error Code: "+e.getResultCode());
                   if(e.getResultCode() == 13){
                        System.out.println("Remove index 0 and retry");
		                client.operate(null, key, 
                        MapOperation.removeByIndex("myMap", 0, retVal));  
                        --i; //Insert again
                   }
	             }	
                 return i;
	}
	public static void main(String[] args) {
		AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);       
		for (int i = 0; i < 123; i++) {
                  System.out.println("Inserting k = "+i);
                  i = insert(client, i);
		}
		client.close();
	}
}
