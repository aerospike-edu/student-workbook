package com.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.RegexFlag;
import com.aerospike.client.query.Statement;
import com.aerospike.client.query.PredExp;

public class ReadContains {
	AerospikeClient client;
	WritePolicy writePolicy;
	QueryPolicy queryPolicy;

	public ReadContains(){
		client = new AerospikeClient(“33.33.33.10", 3000);
		writePolicy = new WritePolicy();
		queryPolicy = new QueryPolicy();
	}

	public void write(Key key, Bin bin1, Bin bin2) {
		client.put(writePolicy, key, bin1, bin2);
		client.createIndex(new Policy(), "test", "Test", "city_idx", "city", IndexType.STRING);
	}

	public void read () {
		Record record = null;
		Statement stmt = new Statement();
		stmt.setSetName("test");
		stmt.setNamespace("test");
		stmt.setIndexName("city_idx");
        	stmt.setPredExp(
            		PredExp.stringBin("city"),
            		PredExp.stringValue("US.*SanDiego"),
            		PredExp.stringRegex(RegexFlag.ICASE | RegexFlag.NEWLINE)
            	);

		RecordSet recordSet = this.client.query(queryPolicy, stmt);
	    while (recordSet.next()) {
	    	record = recordSet.getRecord();
	    	System.out.println(record.toString());
	    }

	 }


	public static void main(String[] args) {
		ReadContains rc= new ReadContains();
		Key key = new Key("test","test",123);
		Bin bin1 = new Bin("city", "US-SanDiego");
		Bin bin2 = new Bin("empid", "123456");
		rc.write(key, bin1, bin2);

		key = new Key("test","test",124);
		bin1 = new Bin("city", "Peru-SanDiego");
		bin2 = new Bin("empid", "784232");
		rc.write(key, bin1, bin2);

		key = new Key("test","test",125);
		bin1 = new Bin("city", "US-LosAngeles");
		bin2 = new Bin("empid", "454231");
		rc.write(key, bin1, bin2);

		rc.read();
	}
}
