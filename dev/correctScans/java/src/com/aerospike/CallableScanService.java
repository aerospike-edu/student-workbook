package com.aerospike;

import java.util.HashSet;
import java.util.Iterator;  //Keep for commented code
import java.util.Set;
import java.util.concurrent.Callable;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;

public class CallableScanService implements Callable<Set<Record>> {
	private AerospikeClient client;
	private int partitionId;
	private int paginateSize;
	private int recordsThisScan=0;
	private int totalRecordsScanned=0;
	private Key lastKey=null;
	private ScanUtils su = new ScanUtils();

	public CallableScanService() {
		super();
	}
	public CallableScanService(AerospikeClient client, int partitionId, int paginateSize) {
		this.client = client;	
		this.partitionId = partitionId;
		this.paginateSize = paginateSize;
	}

	public Set<Record> call() throws Exception {        		
		System.out.println("Running ScanService for partitionId="+ this.partitionId+", paginateSize="+ this.paginateSize);
		Set<Record> rset = new HashSet<Record>();


		if(client!=null){	
			rset = su.scanByPartitionIdWithPagination(client, partitionId, paginateSize);
			recordsThisScan = rset.size();
			totalRecordsScanned += recordsThisScan;
			lastKey = su.getLastKey();
			System.out.println("["+partitionId+"]: -- partial, up to "+ paginateSize +"---------------------" );
			
			//If data has to be consumed in paginateSize, will have to consume here.
			
			//Iterator<Record> recIter = rset.iterator();
			//for(int i=0;i<rset.size();i++){            	  
			//  Record record = recIter.next();
			// System.out.println(record.getValue("tweet") + "\n");
			//}

			while(recordsThisScan >= paginateSize ){
				su.setLastKey(lastKey);
				
				Set<Record> thisSet = su.scanByLastKeyWithPagination(client, paginateSize);
				recordsThisScan = thisSet.size();

				//If data has to be consumed in paginateSize, will have to consume ... and here.
				
				//recIter = thisSet.iterator();
				//for(int i=0;i<thisSet.size();i++){                	  
				//  Record record = recIter.next();
				//  System.out.println(record.getValue("tweet") + "\n");
				//}
	
				//Cannot return lastKey (different data type) back in callable to break the scan in main.

				rset.addAll(thisSet);
				totalRecordsScanned = rset.size();

				//For code validation only - can be deleted.  
				lastKey = su.getLastKey();
				if(recordsThisScan >0){   //cross checking lastKey is tracking partition ids.
					int lastKeyPartitionId = su.getPartitionId(lastKey);
					System.out.println("["+partitionId+"]: -- partial, up to "+ 
							paginateSize +", cross-check lastKeyPartitionId:" + lastKeyPartitionId);
				}
				//End-code validation

			}
			System.out.println("["+partitionId+"]: ************   DONE  *************");

		}
		else{
			System.out.println("NULL client?");
		}

		System.out.println("=========================================================================================");

		System.out.println("PartitionId: " + this.partitionId+", Total Results: ***** "+totalRecordsScanned+" *****");

		System.out.println("=========================================================================================");

		return rset;
	}

}
