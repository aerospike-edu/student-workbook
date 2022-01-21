package com.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.exp.*;
//import com.aerospike.client.Operation;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.Exp.Type;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
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


public class SIExpression {
    public static void main(String[] args) throws Exception {
      SIExpression ce = new SIExpression();
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
        // keyx: name (String)  age (int)
        // Add SI on age bin, if it does not exist already.

        try {
              IndexTask task = client.createIndex(null, "test", "testset",
                                "idx_age", "age", IndexType.NUMERIC);
              task.waitTillComplete(10);
        } 
        catch (AerospikeException e) {
              if(e.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS)
              {  
                  System.out.println(e.toString());
              }
        }
        // Run SI query for records where age >= 30

        RecordSet rs = null;
        
        System.out.println("\n");
        System.out.println("Run SI query for records where age >= 30");

        try {
              Statement stmt = new Statement();
              stmt.setNamespace("test");
              stmt.setSetName("testset");

              // Set Secondary Index filter. 
              // Note: This is not an Expression filter. 
              // That is passed via Policy object, additionally, if needed.
              stmt.setFilter(Filter.range("age", 30L, 150L)); //age >= 30 (and less than 150)
              stmt.setIndexName("idx_age");  //Optional, its existence is implied by setFilter

              String[] bins = {"name", "age"}; //Names of bins to retrieve
              stmt.setBinNames(bins);

              //Execute the SI query
              rs = client.query(null,stmt);

              //Consume the results
              Record r;
              while(rs.next()) {
                r = rs.getRecord();
                System.out.println("Name-Age: "+ r.getValue("name")+ " "+r.getValue("age"));
              }
         }
         finally {
             if(rs!=null) {
                 rs.close();
             }
         }
        
        // Add an Expression filter for names starting with S or s
        // Passed via QueryPolicy object

        QueryPolicy qPolicy = new QueryPolicy();
        qPolicy.filterExp = Exp.build( 
                            Exp.regexCompare("^S.*", RegexFlag.ICASE| RegexFlag.NEWLINE,
                                              Exp.stringBin("name") )
                            ); // names starting with S or s
        // Run SI Query with expression
        try {
              Statement stmt = new Statement();
              stmt.setNamespace("test");
              stmt.setSetName("testset");

              // Set Secondary Index filter. 
              stmt.setFilter(Filter.range("age", 30L, 150L)); //age >= 30 (and less than 150)

              String[] bins = {"name", "age"}; //Names of bins to retrieve
              stmt.setBinNames(bins);

              //Execute the SI query. qPolicy passes the additional Expression filter.
              System.out.println("\n");
              System.out.println("Add another filter condition via Expressions:");
              System.out.println("=== Age >= 30 (SI) AND Names starting with S or s (Exp) ===");
              rs = client.query(qPolicy,stmt);

              //Consume the results
              Record r;
              while(rs.next()) {
                r = rs.getRecord();
                System.out.println("Name-Age: "+ r.getValue("name")+ " "+r.getValue("age"));
              }
              System.out.println("\n");
         }
         finally {
             if(rs!=null) {
                 rs.close();
             }
         }


        // Executing read expression on results of SI query.
        // Return age+200 instead of age

        // Add an Expression filter for names starting with S or s
        // Passed via QueryPolicy object

        qPolicy.filterExp = Exp.build( 
                            Exp.regexCompare("^S.*", RegexFlag.ICASE| RegexFlag.NEWLINE,
                                              Exp.stringBin("name") )
                            ); // names starting with S or s
        // Run queryAggregate with write operation expression

        RecordSet rset = null;
        try {
              Statement stmt = new Statement();
              stmt.setNamespace("test");
              stmt.setSetName("testset");

              // Set Secondary Index filter. 
              stmt.setFilter(Filter.range("age", 30L, 150L)); //age >= 30 (and less than 150)

              String[] bins = {"name", "age", "retval"}; //Names of bins to retrieve
              //String[] bins = {"age"}; //Names of bins to retrieve
              stmt.setBinNames(bins);

              Expression exp = Exp.build(Exp.add(Exp.intBin("age"), Exp.val(200)));

              Operation[] readops = new Operation[1];
              readops[0]= ExpOperation.read("retval", exp, ExpReadFlags.DEFAULT);
              stmt.setOperations(readops);
              stmt.setReturnData(true);

              //Execute the SI query. qPolicy passes the additional Expression filter.
              System.out.println("\n");
              System.out.println("Test ExpOperation.read() - add 200 to age, via query():");
              System.out.println("=== Age >= 30 (SI) AND Names starting with S or s (Exp) ===");
              System.out.println("=== NOTE: RETURNS null - ExpOperation.read() is not allowed in query() ===");

              
              rset = client.query(qPolicy, stmt);
              if(rset!=null){
                while (rset.next()) {
                   Record r = rset.getRecord();
                   System.out.println(r);
                   System.out.println("Name-Age-retval: "+ r.getValue("name")+ " "+r.getValue("age")+" "+r.getValue("retval"));
                }
              } 
        }
        finally {
           if (rset != null) {
             rset.close();
           }
        }

        // Executing read expression on results of SI query.
        // Add new bin, adv_age = age+100

        // Add an Expression filter for names starting with S or s
        // Passed via WritePolicy object

        WritePolicy wPolicy = new WritePolicy(); 
        wPolicy.filterExp = Exp.build( 
                            Exp.regexCompare("^S.*", RegexFlag.ICASE| RegexFlag.NEWLINE,
                                              Exp.stringBin("name") )
                            ); // names starting with S or s
        // Run Execute Task with write operation expression
        try {
              Statement stmt = new Statement();
              stmt.setNamespace("test");
              stmt.setSetName("testset");

              // Set Secondary Index filter. 
              stmt.setFilter(Filter.range("age", 30L, 150L)); //age >= 30 (and less than 150)

              String[] bins = {"name", "age", "adv_age"}; //Names of bins to retrieve
              stmt.setBinNames(bins);

              Expression exp = Exp.build(Exp.add(Exp.intBin("age"), Exp.val(100)));

              Operation[] operations = new Operation[1];
              operations[0]= ExpOperation.write("adv_age", exp, ExpWriteFlags.DEFAULT);
              stmt.setOperations(operations);
              stmt.setReturnData(true);

              //Execute the SI query. qPolicy passes the additional Expression filter.
              System.out.println("\n");
              System.out.println("ExpOperation.write() - Set age+100 in adv_age bin via execute():");
              System.out.println("=== Age >= 30 (SI) AND Names starting with S or s (Exp) ===");
              ExecuteTask et = client.execute(wPolicy, stmt);
              et.waitTillComplete(5,0); 
              System.out.println("Read records with adv_age bin via query():");
              rs = client.query(qPolicy,stmt);

              //Consume the results
              Record r;
              while(rs.next()) {
                r = rs.getRecord();
                System.out.println("Name-Age-Adv_Age: "+ r.getValue("name")+ " "+r.getValue("age") +" "+r.getValue("adv_age"));
              }
              System.out.println("\n");
         }
         finally {
             if(rs!=null) {
                 rs.close();
             }
         }

         

    }

}
