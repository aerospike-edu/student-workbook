package com.aerospike;

import java.util.HashSet;
import java.util.Set;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.PartitionFilter;

public class ScanUtils {
	private int recordsScanned;
	private Key lastKey=null;

	public void setLastKey(Key key){
		this.lastKey = key;
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

	//public int scanByPartitionIdWithPagination(AerospikeClient client, int partitionId, int paginateSize) {
	public Set<Record> scanByPartitionIdWithPagination(AerospikeClient client, int partitionId, int paginateSize) {
		//Scan all records starting with partion id, limiting to paginateSize

		ScanPolicy policy = new ScanPolicy();
		policy.concurrentNodes = false;
		policy.includeBinData = true;
		policy.maxRecords = paginateSize; 
		recordsScanned = 0;
		PartitionFilter partitionFilter = PartitionFilter.id(partitionId);
		final Set<Record> rset = new HashSet<Record>();

		try {
			client.scanPartitions(policy, partitionFilter,  "test", "tweets", new ScanCallback() {
				public void scanCallback(Key key, Record record)
						throws AerospikeException {
					//System.out.println(record.getValue("tweet") + "\n");
					rset.add(record);
					lastKey = key; 
					recordsScanned += 1;
				}
			}, "tweet");

		} catch (AerospikeException e) {
			System.out.println("EXCEPTION - Message: " + e.getMessage());
		}
		//return recordsScanned;
		return rset;
	}
	public Set<Record> scanByLastKeyWithPagination(AerospikeClient client, int paginateSize) {
		//public int scanByLastKeyWithPagination(AerospikeClient client, int paginateSize) {
		//Scan all records starting with lastKey, limiting to paginateSize

		ScanPolicy policy = new ScanPolicy();
		policy.concurrentNodes = false;
		policy.includeBinData = true;
		policy.maxRecords = paginateSize; 
		recordsScanned = 0;
		PartitionFilter partitionFilter = PartitionFilter.after(lastKey);
		final Set<Record> rset = new HashSet<Record>();

		try {
			client.scanPartitions(policy, partitionFilter,  "test", "tweets", new ScanCallback() {
				public void scanCallback(Key key, Record record)
						throws AerospikeException {
					//System.out.println(record.getValue("tweet") + "\n");
					rset.add(record);
					lastKey = key; 
					recordsScanned += 1;
				}
			}, "tweet");

		} catch (AerospikeException e) {
			System.out.println("EXCEPTION - Message: " + e.getMessage());
		}
		//return recordsScanned;
		return rset;
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
	public Key getLastKey() {
		// TODO Auto-generated method stub
		return lastKey;
	}

}
