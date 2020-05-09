package com.aerospike;


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
        }


        public void work() {
		//AerospikeClient client = new AerospikeClient("172.28.128.3", 3000); //Update IP Address 
		AerospikeClient client = new AerospikeClient("127.0.0.1", 3000); //Update IP Address 

	        //Scan all tweets  (no PartitionFilter - regular scan for PartitionFilter Tests and Pagination validation.)
                int totalScannableRecords= scanAllRecordsForAllUsers(client);

                System.out.println("Total Scannable Records: ***** "+totalScannableRecords+" *****");

                // Scan partition by partition, with pagination.
                Key startKey=null;
                int paginateSize = 2;
                int totalRecordsScanned = 0;
                int recordsThisScan = 0;

                for(int partitionId=0; partitionId<4096; partitionId++){
                  if(client!=null){
                    recordsThisScan = scanByPartitionIdWithPagination(client, partitionId, paginateSize);
                    totalRecordsScanned += recordsThisScan;
                    System.out.println("["+partitionId+"]: -- partial, up to "+ paginateSize +"---------------------" );

                    while(recordsThisScan >= paginateSize ){
                        recordsThisScan = scanByLastKeyWithPagination(client, paginateSize);
                        totalRecordsScanned += recordsThisScan;

                        if(recordsThisScan >0){   //cross checking lastKey is tracking partition ids.
                          int lastKeyPartitionId = getPartitionId(lastKey);
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
                
                totalRecordsScanned = 0;
                int scanPartitionRange = 4;

                for(int partitionId=0; partitionId<4096; partitionId+=scanPartitionRange){  //Process 4 partitions at a time.
                  if(client!=null){
                    recordsThisScan = scanByPartitionsRange(client, partitionId, scanPartitionRange); 
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

                client.close();
	}


      public int scanAllRecordsForAllUsers(AerospikeClient client) {
	        //Scan all records.
                ScanPolicy policy = new ScanPolicy();
                policy.concurrentNodes = false;
                policy.includeBinData = true;
                recordsScanned = 0;
                try {
                  client.scanAll(policy, "test", "tweets", new ScanCallback() {

                                public void scanCallback(Key key, Record record)
                                                throws AerospikeException {
                                        //System.out.println(record.getValue("tweet") + "\n");
                                        //Not printing the tweet, just taking the count to crosscheck.
                                        recordsScanned += 1;
                                }
                        }, "tweet");
                } catch (AerospikeException e) {
                        System.out.println("EXCEPTION - Message: " + e.getMessage());
                }
                return recordsScanned;
      } 

      public int scanByPartitionIdWithPagination(AerospikeClient client, int partitionId, int paginateSize) {
	        //Scan all records starting with partion id, limiting to paginateSize
	        
                ScanPolicy policy = new ScanPolicy();
                policy.concurrentNodes = false;
                policy.includeBinData = true;
                policy.maxRecords = paginateSize; 
                recordsScanned = 0;
                PartitionFilter partitionFilter = PartitionFilter.id(partitionId);
                
                try {
                  client.scanPartitions(policy, partitionFilter,  "test", "tweets", new ScanCallback() {
                                public void scanCallback(Key key, Record record)
                                                throws AerospikeException {
                                        System.out.println(record.getValue("tweet") + "\n");
                                        lastKey = key; 
                                        recordsScanned += 1;
                                }
                        }, "tweet");
                  
                } catch (AerospikeException e) {
                        System.out.println("EXCEPTION - Message: " + e.getMessage());
                }
                return recordsScanned;
     }
     public int scanByLastKeyWithPagination(AerospikeClient client, int paginateSize) {
	        //Scan all records starting with lastKey, limiting to paginateSize
	        
                ScanPolicy policy = new ScanPolicy();
                policy.concurrentNodes = false;
                policy.includeBinData = true;
                policy.maxRecords = paginateSize; 
                recordsScanned = 0;
                PartitionFilter partitionFilter = PartitionFilter.after(lastKey);
                
                try {
                  client.scanPartitions(policy, partitionFilter,  "test", "tweets", new ScanCallback() {
                                public void scanCallback(Key key, Record record)
                                                throws AerospikeException {
                                        System.out.println(record.getValue("tweet") + "\n");
                                        lastKey = key; 
                                        recordsScanned += 1;
                                }
                        }, "tweet");
                  
                } catch (AerospikeException e) {
                        System.out.println("EXCEPTION - Message: " + e.getMessage());
                }
                return recordsScanned;
     }
     public int scanByPartitionsRange(AerospikeClient client, int startPartitionId, int scanPartitionRange) {
	        //Scan all records in the specified partition range.
                ScanPolicy policy = new ScanPolicy();
                policy.concurrentNodes = false;
                policy.includeBinData = true;
                policy.maxRecords = 0;  
                recordsScanned = 0;
                
                PartitionFilter partitionFilter = PartitionFilter.range(startPartitionId,scanPartitionRange);  
                
                try {
                  client.scanPartitions(policy, partitionFilter,  "test", "tweets", new ScanCallback() {
                                public void scanCallback(Key key, Record record)
                                                throws AerospikeException {
                                        System.out.println(record.getValue("tweet") + "\n");
                                        lastKey = key;
                                        recordsScanned += 1;
                                }
                        }, "tweet");
                  
                } catch (AerospikeException e) {
                        System.out.println("EXCEPTION - Message: " + e.getMessage());
                }
                return recordsScanned;
     }

     public int restartScanFromKey(AerospikeClient client, Key key) {
	        //Scan all records starting from key
                ScanPolicy policy = new ScanPolicy();
                policy.concurrentNodes = false;
                policy.includeBinData = true;
                policy.maxRecords = 0;  
                recordsScanned = 0;
                
                PartitionFilter partitionFilter = PartitionFilter.after(key);  
                try {
                  client.scanPartitions(policy, partitionFilter,  "test", "tweets", new ScanCallback() {
                                public void scanCallback(Key key, Record record)
                                                throws AerospikeException {
                                        System.out.println(record.getValue("tweet") + "\n");
                                        recordsScanned += 1;
                                }
                        }, "tweet");
                  
                } catch (AerospikeException e) {
                        System.out.println("EXCEPTION - Message: " + e.getMessage());
                }
                return recordsScanned;
     }

     public int getPartitionId(Key key){
                if(key==null) return -1;
                byte[] digest = key.digest;
                int d0 = Byte.toUnsignedInt(digest[0]);
                int d1 = Byte.toUnsignedInt(digest[1]);
                return ((d1&0x0F)<<8)|d0;
     }
}
