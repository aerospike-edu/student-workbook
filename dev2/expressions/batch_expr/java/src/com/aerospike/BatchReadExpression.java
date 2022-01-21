package com.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.exp.*;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.Exp.Type;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.query.RegexFlag;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.Record;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;


public class BatchReadExpression {
    public static void main(String[] args) throws Exception {
      BatchReadExpression ce = new BatchReadExpression();
      ce.runExample();
      return;
    }

    public void runExample() throws AerospikeException {
      AerospikeClient client = new AerospikeClient("127.0.0.1", 3000); //Update IP Address
      runExample1(client);
      client.close();
    }
    private void runExample1(AerospikeClient client) throws AerospikeException {
        // Records were inserted using aql
        // key[0-9]: name (String)  age (int)

        // Run Batch Index Read of key3, key5 and key7

        //RecordSet rs = null;
        Key [] keys = new Key[3];
        String[] userkey = {"key3", "key5", "key7"};
        for(int i=0;i<3;i++) {
          keys[i]= new Key("test", "testset", userkey[i]);
        }

        //Names of bins to retrieve
        String[] bins = {"name", "age"}; 

        System.out.println("\n");
        System.out.println("Read key3, key5 & key7 as batch index read");
        Record[] rs = null;

        //Execute batch index read
        rs = client.get(null,keys,bins);

        for(int i=0; i<3; i++) {
           Record r = rs[i];
           System.out.println(userkey[i]+": Name-Age: "+ r.getValue("name")+ " "+r.getValue("age"));
        }
        
        // Add an Expression filter for names starting with J or j 
        // Passed via QueryPolicy object

        BatchPolicy bPolicy = new BatchPolicy();
        bPolicy.filterExp = Exp.build( 
                            Exp.regexCompare("^J.*", RegexFlag.ICASE| RegexFlag.NEWLINE,
                                              Exp.stringBin("name") )
                            ); // names starting with J or j
        // Run Batch Index Read with expression
        System.out.println("\n");
        System.out.println("Read key3, key5 & key7 as batch index read, \nwith filter Expression for names starting with J");
        rs = client.get(bPolicy,keys,bins);
        for(int i=0; i<3; i++) {
           Record r = rs[i];
           if(r!=null) {
             System.out.println(userkey[i]+": Name-Age: "+ r.getValue("name")+ " "+r.getValue("age"));
           }
        }


        // Using ExpOperation.read() in addition to filter Expression in batch index reads.

        // Add an Expression filter for names starting with J or j 
        // Passed via QueryPolicy object

        bPolicy.filterExp = Exp.build( 
                            Exp.regexCompare("^J.*", RegexFlag.ICASE| RegexFlag.NEWLINE,
                                              Exp.stringBin("name") )
                            ); // names starting with J or j

        // Add ExpOperation.read() - add 200 to age are return as retval
        Expression exp = Exp.build(Exp.add(Exp.intBin("age"), Exp.val(200)));

        // Run Batch Index Read with expression
        System.out.println("\n");
        System.out.println("Read key3, key5 & key7 as batch index read, \nwith filter Expression for names starting with J");
        System.out.println("Add 200 to age and return along with name and age - demonstrate ExpOperation.read()");

        Operation[] readops = new Operation[3];

        //readops[0]= ExpOperation.write("retval", exp, ExpWriteFlags.DEFAULT); 
        //Note: Will cause Exception. ExpOperation.write() not allowed in batch index reads.

        readops[0]= ExpOperation.read("retval", exp, ExpReadFlags.DEFAULT);
        readops[1]= Operation.get("name");
        readops[2]= Operation.get("age");

        rs = client.get(bPolicy,keys,readops);
        for(int i=0; i<3; i++) {
           Record r = rs[i];
           if(r!=null) {
             System.out.println(userkey[i]+": Name-Age-Age+200: "+ r.getValue("name")+ " "+r.getValue("age") +" "+r.getValue("retval"));
           }
        }
    }
}
