package com.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.exp.*;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Exp.Type;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

public class WriteOpExpression {
    public static void main(String[] args) throws Exception {
      WriteOpExpression ce = new WriteOpExpression();
      ce.runExample();
      return;
    }

    public void runExample() throws Exception {
      AerospikeClient client = new AerospikeClient("127.0.0.1", 3000); //Update IP Address
      runExample4(client);
    }
    private void runExample4(AerospikeClient client) throws Exception {
        // Set bin2+bin3 as bin4 if bin1>5 and LUT > xxx 
        System.out.println("\nSet bin2+bin3 as bin4 if bin1>5 and LUT > xxx");

        Key key = new Key("test", "testset", "getkey");
        Bin bin1 = new Bin("bin1", 8);
        Bin bin2 = new Bin("bin2", 2);
        Bin bin3 = new Bin("bin3", 3);
        //Bin bin4 = new Bin("bin4", 3);

        client.delete(null, key);

        client.put(null, key, bin1, bin2, bin3);
        //client.put(null, key, bin1, bin2, bin3, bin4);
        Record record = client.get(null, key);

        System.out.println("Initial Record: " + record);

        // Note: ExpOperation are executed in operate() which requires WritePolicy

        WritePolicy wPolicy = new WritePolicy(client.writePolicyDefault);
        long lut_01_01_2022 = 1641027600000000000L;
        long lut_01_01_2032 = 1956560400000000000L;

        wPolicy.filterExp = Exp.build( Exp.and(
                                        Exp.gt(Exp.bin("bin1", Type.INT), Exp.val(5)) ,
                                        Exp.gt(Exp.lastUpdate(), Exp.val(lut_01_01_2032))  
                                       )
                                     );
        // Write with expression operation using operate
        Expression exp = Exp.build(Exp.add(Exp.intBin("bin2"), Exp.intBin("bin3")));

        record = client.operate(wPolicy, key, 
                                 ExpOperation.write("bin4", exp, ExpWriteFlags.DEFAULT)
                               ); 
                                // Explore other ExpWriteFlags
        System.out.println("Set bin2+bin3=5 as bin4, if bin1>5 & LUT> 1-1-2032 -expr is false:\n" + record);

        record = client.get(null,key);
        System.out.println("Get Record: " + record);

        wPolicy.filterExp = Exp.build( Exp.and(
                                        Exp.gt(Exp.bin("bin1", Type.INT), Exp.val(5)) ,
                                        Exp.gt(Exp.lastUpdate(), Exp.val(lut_01_01_2022))  
                                       )
                                     );
        // Write with expression operation using operate
        record = client.operate(wPolicy, key, 
                                 ExpOperation.write("bin4", exp, ExpWriteFlags.DEFAULT)
                                 //, Operation.get("bin4")
                               ); 
        System.out.println("Set bin2+bin3=5 as bin4, if bin1>5 & LUT> 1-1-2022 -expr is true:\n" + record);

        record = client.get(null,key);
        System.out.println("Get Record: " + record);

    }
}
