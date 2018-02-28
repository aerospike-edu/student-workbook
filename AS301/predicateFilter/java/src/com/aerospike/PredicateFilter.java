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
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.PredExp;

public class PredicateFilter {
	AerospikeClient client;
	WritePolicy writePolicy;
	QueryPolicy queryPolicy;

	public PredicateFilter(){
		client = new AerospikeClient(null, "127.0.0.1", 3000);
		writePolicy = new WritePolicy();
		queryPolicy = new QueryPolicy();
	        try  {
	          client.createIndex(new Policy(), "test", "testset", "country_idx", "country", IndexType.STRING);
                }
                catch(Exception e){
                }
	}

	public void write(Key key, Bin bin1, Bin bin2, Bin bin3) {
		client.put(writePolicy, key, bin1, bin2, bin3);
	}

	public void read () {
		Record record = null;
		Statement stmt = new Statement();
		stmt.setSetName("testset");
		stmt.setNamespace("test");
		stmt.setIndexName("country_idx");
		stmt.setFilter(Filter.equal("country","USA"));
        	stmt.setPredExp(
            		PredExp.stringBin("city"),
            		PredExp.stringValue("U.*Diego"),
            		PredExp.stringRegex(RegexFlag.ICASE | RegexFlag.NEWLINE)
            	);

		RecordSet recordSet = this.client.query(queryPolicy, stmt);
	    while (recordSet.next()) {
	    	record = recordSet.getRecord();
	    	System.out.println(record.toString());
	    }

	 }


	public static void main(String[] args) {
		PredicateFilter rc= new PredicateFilter();
		Key key = new Key("test","testset",123);
		Bin bin1 = new Bin("city", "US-SanDiego");
		Bin bin2 = new Bin("empid", "123456");
		Bin bin3 = new Bin("country", "USA");
		rc.write(key, bin1, bin2, bin3);

		key = new Key("test","testset",124);
		bin1 = new Bin("city", "Peru-SanDiego");
		bin2 = new Bin("empid", "784232");
		bin3 = new Bin("country", "Peru");
		rc.write(key, bin1, bin2, bin3);

		key = new Key("test","testset",125);
		bin1 = new Bin("city", "US-LosAngeles");
		bin2 = new Bin("empid", "454231");
		bin3 = new Bin("country", "USA");
		rc.write(key, bin1, bin2, bin3);

		rc.read();
	}
}
