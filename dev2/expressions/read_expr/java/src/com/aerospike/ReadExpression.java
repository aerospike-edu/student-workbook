package com.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.exp.*;
import com.aerospike.client.exp.Exp.Type;
import com.aerospike.client.policy.Policy;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;

public class ReadExpression {
    public static void main(String[] args) throws Exception {
      ReadExpression ce = new ReadExpression();
      ce.runExample();
      return;
    }

    public void runExample() throws Exception {
      AerospikeClient client = new AerospikeClient("127.0.0.1", 3000); //Update IP Address
      runExample1(client);
    }
    private void runExample1(AerospikeClient client) throws AerospikeException {
        // Read bin3 if bin1>5 and LUT > expression for get
        System.out.println("\nRead bin3 if bin1>5 and LUT > xxx");

        Key key = new Key("test", "testset", "getkey");
        Bin bin1 = new Bin("bin1", 8);
        Bin bin2 = new Bin("bin2", "v2");
        Bin bin3 = new Bin("bin3", "v3");

        client.delete(null, key);

        client.put(null, key, bin1, bin2, bin3);
        Record record = client.get(null, key);

        System.out.println("\nInitial Record: " + record);

        Policy rPolicy = new Policy(client.readPolicyDefault);
        long lut_01_01_2022 = 1641027600000000000L;
        long lut_01_01_2032 = 1956560400000000000L;

        rPolicy.filterExp = Exp.build( Exp.and(
                                        Exp.gt(Exp.bin("bin1", Type.INT), Exp.val(5)) ,
                                        Exp.gt(Exp.lastUpdate(), Exp.val(lut_01_01_2022))  
                                       )
                                     );
        // Read with expression (expected =true)
        record = client.get(rPolicy, key, "bin3");
        System.out.println("\nGet bin3=v3 if bin1>5 & LUT> 1-1-2022 expr is true: " + record);

        // Read with expression (expected =false)
        System.out.println("\nRead with Expression(expected =false) with failOnFiteredOut = true.");
        record = null;
        rPolicy.filterExp = Exp.build( Exp.and(
                                        Exp.gt(Exp.bin("bin1", Type.INT), Exp.val(5)) ,
                                        Exp.gt(Exp.lastUpdate(), Exp.val(lut_01_01_2032))  
                                       )
                                     );
        rPolicy.failOnFilteredOut = true;
        try {
          record = client.get(rPolicy, key, "bin3");
        }
        catch (AerospikeException e){
          int rc= e.getResultCode();
          if(rc == ResultCode.FILTERED_OUT) {
            System.out.println("Exception Caught: Record Filtered Out.");
            //System.out.println(e.toString());
          }
        } 
        //No exception raised if failOnFilteredOut = false. Record returns null.
        System.out.println("\nRead with Expression(expected =false) with failOnFiteredOut = false.");
        record = client.get(null,key,"bin3"); //record is non-null now.
        rPolicy.failOnFilteredOut = false;
        record = client.get(rPolicy, key, "bin3");
        System.out.println("Returns null, bin1>5 & LUT> 1-1-2032 expr is false: " + record);
        

        System.out.println("\nbin1>5 & LUT> 1-1-2032 expr is false. \nfailOnFiteredOut=true but not catching Exception: ");

        //Exception raised if failOnFilteredOut = true. Here, we did not catch it.
        record = client.get(null,key,"bin3");
        rPolicy.failOnFilteredOut = true;
        record = client.get(rPolicy, key, "bin3");  //<-- Runs into exception failure here.
        System.out.println("Returns null, bin1>5 & LUT> 1-1-2032 expr is false: " + record);
    }
}
