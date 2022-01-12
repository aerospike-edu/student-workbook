package com.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.exp.*;
import com.aerospike.client.exp.Exp.Type;
import com.aerospike.client.policy.WritePolicy;

public class WriteExpression {
    public static void main(String[] args) throws Exception {
      WriteExpression ce = new WriteExpression();
      ce.runExample();
      return;
    }

    public void runExample() throws Exception {
      AerospikeClient client = new AerospikeClient("127.0.0.1", 3000); //Update IP Address
      runExample3(client);
    }
    private void runExample3(AerospikeClient client) throws Exception {
        // Insert/Update bin3=10 if bin1>5 and LUT > expression for get
        System.out.println("\nUpdate/Add bin3=10 if bin1>5 and LUT > xxx");

        Key key = new Key("test", "testset", "getkey");
        Bin bin1 = new Bin("bin1", 8);
        Bin bin2 = new Bin("bin2", "v2");

        client.delete(null, key);

        client.put(null, key, bin1, bin2);
        Record record = client.get(null, key);

        System.out.println("Initial Record: " + record);

        WritePolicy wPolicy = new WritePolicy(client.writePolicyDefault);
        long lut_01_01_2022 = 1641027600000000000L;
        long lut_01_01_2032 = 1956560400000000000L;

        wPolicy.filterExp = Exp.build( Exp.and(
                                        Exp.gt(Exp.bin("bin1", Type.INT), Exp.val(5)) ,
                                        Exp.gt(Exp.lastUpdate(), Exp.val(lut_01_01_2022))  
                                       )
                                     );
        // Put with expression
        Bin bin3 = new Bin("bin3", 10);
        client.put(wPolicy, key, bin3);
        record = client.get(null, key);
        System.out.println("Set bin3=10 if bin1>5 & LUT> 1-1-2022 expr is true.\nRecord: " + record);

        wPolicy.filterExp = Exp.build( Exp.and(
                                        Exp.gt(Exp.bin("bin1", Type.INT), Exp.val(5)) ,
                                        Exp.gt(Exp.lastUpdate(), Exp.val(lut_01_01_2032))  
                                       )
                                     );
        // Put with expression
        bin3 = new Bin("bin3", 12);
        client.put(wPolicy, key, bin3);
        record = client.get(null, key);
        System.out.println("bin3=10 --> 12 fails?, bin1>5 & LUT> 1-1-2032 expr is false.\nRecord: " + record);
    }
}
