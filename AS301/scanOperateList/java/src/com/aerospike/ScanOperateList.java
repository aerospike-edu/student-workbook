package com.aerospike;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
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
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.Priority;
import com.aerospike.client.ScanCallback;

public class ScanOperateList {
	public static void appendUnordered(AerospikeClient client, int value, int pk) {
		Key key = new Key("test", "s1", pk);  
		client.operate(null, key, 
                       ListOperation.append("myList", Value.get(value))
                       );
	}

	public static void main(String[] args) {
		final AerospikeClient client = new AerospikeClient("127.0.0.1", 3000) ;
		
		WritePolicy policy = new WritePolicy();
		policy.recordExistsAction = RecordExistsAction.UPDATE;

                //Create 5 records with unordered list
                for (int j=1; j<=5; j++){		
		  Key key = new Key("test", "s1", j);
		  client.delete(policy, key);
                  client.put(policy, key, new Bin("id", j));
		  for (int i = 10; i < 20; i++) {
			appendUnordered(client, i+j, j);
		  }
		  for (int i = 0; i < 10; i++) {
			appendUnordered(client, i, j);
		  }
                }

                //Print the records 
		System.out.println("Records created, unordered list:");
                for (int j=1; j<=5; j++){		
		  Key key = new Key("test", "s1", j);
		  System.out.println(client.get(policy, key, "myList"));
                } 

                //Launch scan job to modify each returned record

                ScanPolicy scanPolicy = new ScanPolicy();
                scanPolicy.concurrentNodes = false;
                scanPolicy.priority = Priority.LOW;
                scanPolicy.includeBinData = false;

                client.scanAll(scanPolicy, "test", "s1", new ScanCallback(){
                  public void scanCallback(Key key, Record record) throws AerospikeException{
                    client.operate(null, key, ListOperation.setOrder("myList", ListOrder.ORDERED));
                  }
                });   

                //Print the records 
		System.out.println("After scan processing ...");
                for (int j=1; j<=5; j++){		
		  Key key = new Key("test", "s1", j);
		  System.out.println(client.get(policy, key, "myList"));
                } 

		client.close();
	}
}
