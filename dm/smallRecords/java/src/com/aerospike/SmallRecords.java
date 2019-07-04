package com.aerospike;

import java.util.HashMap;
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.util.ThreadLocalData;

import gnu.crypto.hash.RipeMD160;


public class SmallRecords
{
	private AerospikeClient client;
    public SmallRecords()
    {
    	//Constructor - connect to Aerospike Cluster
    	
    	try {
          System.out.println( "Opening Connection to Aerospike Server." );
          this.client = new AerospikeClient("127.0.0.1", 3000);
          if(client==null || !client.isConnected()){
        	System.out.println( "Error opening Connection to Aerospike Server." );
          }          
    	}
    	catch(AerospikeException e){
    		System.out.println(e.getMessage());    		
    	}
    }
    
    public static void main( String[] args )
    {
    	SmallRecords app = new SmallRecords();   //Establishes connection
    	
    	//We will insert 50 individual records in smallRecordsSet
    	//and the same records as k:v map items in largeRecordSet - approx 8 per record.
    	//We will take RIPEMD160 hash of the record key, BITWISE AND it with 0x07 
    	//resulting in 8 unique keys where we will store these records as map items.
    	
    	int start=0;
    	int end = 50;
    	
    	app.insertSmallRecords(start, end);
    	app.readSmallRecords(start, end);
    	//app.deleteSmallRecords(start, end);  //for testing
    	
    	app.insertLargeRecords(start, end);
    	app.readLargeRecords(start, end);
    	
    	//System.out.println( "Checking RIPEMD160 hash computation on string: \"test\"" );
    	//app.checkHashFunction();  //for testing
    	
    	if(app.client != null && app.client.isConnected()){
			app.client.close();
			System.out.println( "Connection to Aerospike Server closed." );
    	}
    }
    
    
    public void insertSmallRecords(int start, int end){ 		
		for (int i =start; i < end; i++) {
		  Key key = new Key("test", "smallRecordsSet", "id"+i);
		  this.client.put(null, key, new Bin("id", "id"+i), new Bin("name", "name"+i), new Bin ("ver", i));
		}
    }
    
    public void readSmallRecords(int start, int end){
    		
		for (int i = start; i < end; i++) {
		  Key key = new Key("test", "smallRecordsSet", "id"+i);
		  System.out.println(this.client.get(null, key));
		}
    }
    public void deleteSmallRecords(int start, int end){
		
		for (int i = start; i < end; i++) {
		  Key key = new Key("test", "smallRecordsSet", "id"+i);
		  System.out.println(this.client.delete(null, key));
		}
    }
    public void insertLargeRecords(int start, int end){ 		
		for (int i =start; i < end; i++) {
			
		  byte[] hashedKey = getHash("id"+i);		  
		  hashedKey[0] = (byte) (hashedKey[0] & 0x07);  //Use only 7 unique keys
		  for(int j = 1; j<20;j++){hashedKey[j]=0;}
		  
		  Key key = new Key("test", "largeRecordsSet", hashedKey);
		  
		  
		  HashMap<Object,Object> recordBinsMap = new HashMap<Object,Object>();
		  recordBinsMap.put("id","id"+i);
		  recordBinsMap.put("name","name"+i);
		  recordBinsMap.put("ver", i);
		  
		  
		  Map<Value,Value> recordMap = new HashMap<Value,Value>();
		  recordMap.put(Value.get("id"+i), Value.get(recordBinsMap));
		  
		  MapPolicy mapPolicy = new MapPolicy(); 
		  this.client.operate(null, key, MapOperation.putItems(mapPolicy, "records", recordMap));
		}
    }
    
    public void readLargeRecords(int start, int end){ 		
		for (int i =start; i < end; i++) {
			
		  byte[] hashedKey = getHash("id"+i);		  
		  hashedKey[0] = (byte) (hashedKey[0] & 0x07);  //Use only 7 unique keys
		  for(int j = 1; j<20;j++){hashedKey[j]=0;}
		  
		  Key key = new Key("test", "largeRecordsSet", hashedKey);
		  
		  Record outStr = this.client.operate(null, key, 
				  MapOperation.getByKey("records", Value.get("id"+i), MapReturnType.VALUE));
		  
		  System.out.println(outStr);
		  
		  //Or you can just print specific bin value from the record:
		  //System.out.println(outStr.getValue("records"));
		}
    }
    
    public byte[] getHash(String str){
		
    	byte[] buffer = ThreadLocalData.getBuffer();
		int strLength = Buffer.stringToUtf8(str, buffer, 0);

		RipeMD160 hash = new RipeMD160();
		hash.update(buffer, 0, strLength);		
		return hash.digest();
    }
     
    public void checkHashFunction(){
    	byte[]  hashTest = getHash("test");
    	//test: digest=5e:52:fe:e4:7e:6b:07:05:65:f7:43:72:46:8c:dc:69:9d:e8:91:07
    	//Check
    	for(int i=0; i<20;i++){
    	  if(i<19){
    		  System.out.printf("%02x:",hashTest[i]);
    	  }
    	  else{
    		  System.out.printf("%02x",hashTest[i]);
    	  }
        }
    	System.out.println("\n5e:52:fe:e4:7e:6b:07:05:65:f7:43:72:46:8c:dc:69:9d:e8:91:07");
    }
}

