package com.aerospike;


import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Record;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.Language;

public class CorrectScans {


	//Tracking counter, updated in the call back. 
	private int recordsScanned = 0;

	//Track last key scanned, updated in the call back. 
	private Key lastKey = null;



	public static void main(String[] args) {

		CorrectScans cs = new CorrectScans();
		cs.work();
		return;
	}


	public void work() {
		//AerospikeClient client = new AerospikeClient("127.0.0.1", 3000); //Update IP Address 
		AerospikeClient client = new AerospikeClient("35.175.131.62", 3000); //Update IP Address 
		ScanUtils su = new ScanUtils();
		su.setLastKey(null);

		//Scan all tweets  (no PartitionFilter - regular scan for PartitionFilter Tests and Pagination validation.)
		int totalScannableRecords= su.scanAllRecordsForAllUsers(client);

		System.out.println("Total Scannable Records: ***** "+totalScannableRecords+" *****");

		// Scan partition by partition, with pagination.
		Key startKey=null;
		int paginateSize = 10;
		int totalRecordsScanned = 0;
		int recordsThisScan = 0;
		int scanPartitionRange = 0;
		int maxPartitions = 4096;  //Set to 4 or 8 for validation tests.

		for(int partitionId=0; partitionId<maxPartitions; partitionId++){
			if(client!=null){
				recordsThisScan = (su.scanByPartitionIdWithPagination(client, partitionId, paginateSize)).size();
				totalRecordsScanned += recordsThisScan;
				lastKey = su.getLastKey();
				System.out.println("["+partitionId+"]: -- partial, up to "+ paginateSize +"---------------------" );
				//Just scanning for size for comparison. Data -> consume in ScanUtils function.

				while(recordsThisScan >= paginateSize ){
					su.setLastKey(lastKey);
					recordsThisScan = (su.scanByLastKeyWithPagination(client, paginateSize)).size();
					//Just scanning for size for comparison. Data -> consume in ScanUtils function.
					totalRecordsScanned += recordsThisScan;
					lastKey = su.getLastKey();
					if(recordsThisScan >0){   //cross checking lastKey is tracking partition ids.
						int lastKeyPartitionId = su.getPartitionId(lastKey);
						System.out.println("["+partitionId+"]: -- partial, up to "+ 
								paginateSize +", cross-check lastKeyPartitionId:" + lastKeyPartitionId);
					}

				}
				System.out.println("["+partitionId+"]: ************   DONE  *************");
			}
			else{
				System.out.println("NULL client?");
			}
		}
		System.out.println("=========================================================================================");

		System.out.println("Scan one partition at a time, with pagination. Total Results: ***** "+totalRecordsScanned+" *****");
		System.out.println("Total Expected Results: ***** "+totalScannableRecords+" *****");
		System.out.println("=========================================================================================");



		// Range Scan: Scan 4 partitions at a time, return all records in the 4 partitions. (No pagination)
		//Enable later
		/*
                totalRecordsScanned = 0;
                int scanPartitionRange = 4;


                for(int partitionId=0; partitionId<maxPartitions; partitionId+=scanPartitionRange){  //Process 4 partitions at a time.
                  if(client!=null){
                    recordsThisScan = su.scanByPartitionsRange(client, partitionId, scanPartitionRange); 
                    totalRecordsScanned += recordsThisScan;
                    System.out.println("["+partitionId+"]: ********************");
                  }
                  else{
                    System.out.println("NULL client?");
                  }
                }


                System.out.println("=========================================================================================");
                System.out.println("Total by "+scanPartitionRange+" partitions at a time, with pagination: ***** "+totalRecordsScanned+" *****");
                System.out.println("Total actual tweets: ***** "+totalScannableRecords+" *****");
                System.out.println("=========================================================================================");

		 */
		//Range (of partitions) Scan with pagination ( n or less records at a time for each partition).  

		System.out.println("===SCAN USING A CALLABLE THREADED SCAN SERVICE===");
		
		maxPartitions = 4096;
		scanPartitionRange = 100; 
		paginateSize = 10;
		//Have a thread per partition available
		ExecutorService pool  = Executors.newFixedThreadPool(scanPartitionRange);
		Set<Record> rset = new HashSet<Record>();  
		Set<Future<Set<Record>>> set = new HashSet<Future<Set<Record>>>();

		for(int partitionStep=0; partitionStep<maxPartitions; partitionStep+=scanPartitionRange){ 
			System.out.println("Scanning service -- partitionStep = :"+ partitionStep);
			int partitionId = 0;
			for(partitionId=partitionStep; partitionId<(partitionStep+scanPartitionRange); partitionId++){
				System.out.println("Scanning service -- partitionId = :"+ partitionId);
				Callable<Set<Record>> callable = new CallableScanService(client, partitionId, paginateSize);

				Future<Set<Record>> future = pool.submit(callable);
				set.add(future);
			}
			for (Future<Set<Record>> future : set) {
				try{
					rset.addAll(future.get());
				}
				catch (Exception e) {
					System.out.println("EXCEPTION - Message: " + e.getMessage());
				}
			}

			//Output results for scanPartitionRange 
			System.out.println("Scanned thru partition: "+ partitionId + ". Total tweets:"+ rset.size() + "\n");
			Iterator<Record> recIter = rset.iterator();

			for(int i=0;i<rset.size();i++){            	  
				Record record = recIter.next();
				System.out.println("TWEET Data: " + record.getValue("tweet") + "\n");
			}

			//If you want to consume data in paginateSize chunks only, consume in CallableScanService.
			
			//Reset containers for next scanPartitionRange
			set.clear();
			rset.clear();
		}  //Loop


		System.out.println("Scanning service done");
		client.close();
	}

}
