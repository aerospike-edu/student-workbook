package com.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.exp.*;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Exp.Type;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public class ReadOpExpression {
    public static void main(String[] args) throws Exception {
      ReadOpExpression ce = new ReadOpExpression();
      ce.runExample();
      return;
    }

    public void runExample() throws Exception {
      AerospikeClient client = new AerospikeClient("127.0.0.1", 3000); //Update IP Address
      runExample2(client);
    }
    private void runExample2(AerospikeClient client) throws Exception {
        // Get bin2+bin3 if bin1>5 and LUT > xxx
        System.out.println("\nGet bin2+bin3 if bin1>5 and LUT > xxx");

        Key key = new Key("test", "testset", "getkey");
        Bin bin1 = new Bin("bin1", 8);
        Bin bin2 = new Bin("bin2", 2);
        Bin bin3 = new Bin("bin3", 3);

        client.delete(null, key);

        client.put(null, key, bin1, bin2, bin3);
        Record record = client.get(null, key);

        System.out.println("Initial Record: " + record);

        // Note: ExpOperation are executed in operate() which requires WritePolicy

        WritePolicy wPolicy = new WritePolicy(client.writePolicyDefault);
        long lut_01_01_2022 = 1641027600000000000L;
        long lut_01_01_2032 = 1956560400000000000L;

        wPolicy.filterExp = Exp.build( Exp.and(
                                        Exp.gt(Exp.bin("bin1", Type.INT), Exp.val(5)) ,
                                        Exp.gt(Exp.lastUpdate(), Exp.val(lut_01_01_2022))  
                                       )
                                     );
        // Read with expression operation using operate
        Expression exp = Exp.build(Exp.add(Exp.intBin("bin2"), Exp.intBin("bin3")));

        record = client.operate(wPolicy, key, 
                                 ExpOperation.read("retval", exp, ExpReadFlags.DEFAULT)
                               ); 
                                // Explore other ExpReadFlags
        System.out.println("Get bin2+bin3=5, if bin1>5 & LUT> 1-1-2022 expr is true: " + record);

        wPolicy.filterExp = Exp.build( Exp.and(
                                        Exp.gt(Exp.bin("bin1", Type.INT), Exp.val(5)) ,
                                        Exp.gt(Exp.lastUpdate(), Exp.val(lut_01_01_2032))  
                                       )
                                     );
        // Read with expression operation using operate
        record = client.operate(wPolicy, key, 
                                 ExpOperation.read("retval", exp, ExpReadFlags.DEFAULT)
                               ); 
        System.out.println("Returns null, bin1>5 & LUT> 1-1-2032 expr is false: " + record);
    }
}
