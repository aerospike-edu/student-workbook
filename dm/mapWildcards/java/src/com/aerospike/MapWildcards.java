package com.aerospike;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.AerospikeClient;
//import com.aerospike.client.Record;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.ListWriteFlags;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapWriteMode;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

public class MapWildcards {
	public static void putItemsUnOrderedMap(AerospikeClient client, Key key, Map<Value,Value> m) {
		MapPolicy mPolicy = new MapPolicy(MapOrder.UNORDERED, MapWriteMode.UPDATE); 
		client.operate(null, key, MapOperation.putItems(mPolicy, "myMap", m));
		} //used
	
	public static void putItemsUnOrderedList(AerospikeClient client, Key key, List<Value> l) {
		ListPolicy lPolicy = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.DEFAULT);
		client.operate(null, key, ListOperation.appendItems(lPolicy, "myList", l));
		} //used
        
   public static void putUniqueItemsUnOrderedList(AerospikeClient client, Key key, List<Value> l) {
	   ListPolicy lPolicy = new ListPolicy(ListOrder.UNORDERED, 
			   ListWriteFlags.ADD_UNIQUE | ListWriteFlags.PARTIAL | ListWriteFlags.NO_FAIL);
	   client.operate(null, key, ListOperation.appendItems(lPolicy, "myList", l));
	   }

	public static void main(String[] args) {
		AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);
		
		//Insert 3 records, with MapPolicy UNORDERED 
		Key key1 = new Key("test", "s1", 1);
		Key key2 = new Key("test", "s1", 2);
		
		WritePolicy policy = new WritePolicy();
		policy.recordExistsAction = RecordExistsAction.UPDATE;
		
		client.delete(policy, key1);
		client.delete(policy, key2);
		

        client.put(policy, key1, new Bin("id","ID1"));
        client.put(policy, key2, new Bin("id","ID2"));
        

        Map<Value, Value> m1 = new HashMap<Value, Value>();
        
        // Map Values are a list of items  [hobby, age, town]
        List<Value> v1 = new ArrayList<Value>();
        v1.add(Value.get("Baseball"));
        v1.add(Value.get(56));
        v1.add(Value.get("San Jose"));
        m1.put(Value.get("Jim"), Value.get(v1));
        
        List<Value> v2 = new ArrayList<Value>();
        v2.add(Value.get("Cricket"));
        v2.add(Value.get(51));
        v2.add(Value.get("Mountain View"));        
        m1.put(Value.get("John"), Value.get(v2));
        
        List<Value> v3 = new ArrayList<Value>();
        v3.add(Value.get("Hiking"));
        v3.add(Value.get(31));
        //v3.add(Value.get("Mountain View"));
        m1.put(Value.get("Jill"), Value.get(v3));
        
        List<Value> v4 = new ArrayList<Value>();
        v4.add(Value.get("Swimming"));
        v4.add(Value.get(36));
        v4.add(Value.get("San Jose"));
        m1.put(Value.get("Jack"), Value.get(v4));
        
        List<Value> v5 = new ArrayList<Value>();
        v5.add(Value.get("Basketball"));
        v5.add(Value.get(26));
        //v5.add(Value.get("Danville"));
        m1.put(Value.get("Nik"), Value.get(v5));
        
	    putItemsUnOrderedMap(client, key1, m1); 
	    
	    System.out.println("\nRecords inserted (map key insert order: Jim, John, Jill, Jack, Nik):");
	    
	    for(int i=0; i<5;i++){
	    	System.out.println("\nKey1, KEY_UNORDERED, getByIndex("+i+") = "+
	            client.operate(null, key1,
	    		MapOperation.getByIndex("myMap", i, MapReturnType.KEY_VALUE)));
	    	}
	    
	    List<Value> l1 = new ArrayList<Value>();
	    //l1.add(Value.WILDCARD);
	    l1.add(Value.get("Bask"));
	    l1.add(Value.WILDCARD);
	    
	    List<Value> l2 = new ArrayList<Value>();
	    l2.add(Value.get("G"));
	    l2.add(Value.WILDCARD);
	    
	    System.out.println("\nValue Range Query (includes LOW, <HIGH):"); 
	    
	    System.out.println("\nKey1, getByValueRange([\"Bask\",*],[\"G\",*]) = "+ 
	    client.operate(null, key1, 
	    		MapOperation.getByValueRange("myMap", Value.get(l1), Value.get(l2), MapReturnType.KEY_VALUE)
	    		));
	    
	    List<Value> itemList = new ArrayList<Value>();
	    itemList.add(Value.get("Baseball"));
	    itemList.add(Value.WILDCARD);
	    
	    System.out.println("\nValue Query (uses WILDCARD):"); 
	    System.out.println("\nKey1, getByValue('Baseball in List') = "+	    
	    		client.operate(null, key1,  
	    				MapOperation.getByValue("myMap", Value.get(itemList), MapReturnType.KEY_VALUE)
	    				));
	    System.out.println("\nValue Query (uses INFINITY):"); 
	    
	    System.out.println("\nKey1, getByKeyRange('Jo' thru INFINITY) = "+ 
	    client.operate(null, key1,
	    		MapOperation.getByKeyRange("myMap", Value.get("Jo"),Value.INFINITY, MapReturnType.KEY_VALUE)
	    		));                
               
	    //List example
	    List<Value> listItems = new ArrayList<Value>();
	    listItems.add(Value.get("Basketball"));
	    listItems.add(Value.get(26));
	    listItems.add(Value.get("Danville"));
	    listItems.add(Value.get(26));
	    listItems.add(Value.get(2));
	    listItems.add(Value.get(6));
	    
	    putItemsUnOrderedList(client, key2, listItems);	    
	    
	    System.out.println("\n List Type Record inserted:"); 
	    
	    System.out.println("\nKey2, UNORDERED UNIQUE List= "+ 	    
	    		client.operate(null, key2, Operation.get("myList")));
	    
	    System.out.println("\nList Value Query (uses INFINITY):"); 
	    System.out.println("\nKey2, getByValueRange(7 thru INFINITY) = "+ 
	    client.operate(null, key2, 
	    		ListOperation.getByValueRange("myList", Value.get(7),Value.INFINITY, ListReturnType.VALUE)
	    		));
	    client.close();
	    }
}
