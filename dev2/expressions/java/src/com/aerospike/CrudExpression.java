package com.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.*;
import com.aerospike.client.operation.HLLOperation;
import com.aerospike.client.operation.HLLPolicy;
import com.aerospike.client.operation.HLLWriteFlags;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CrudExpression {
    public static void main(String[] args) throws Exception {
      CrudExpression ce = new CrudExpression();
      ce.runExample();
      return;
    }

    public void runExample() throws Exception {
      AerospikeClient client = new AerospikeClient("127.0.0.1", 3000); //Update IP Address
      runExample1(client);
      runExample2(client);
      runExample3(client);
      runExample4(client);
      runExample5(client);
      runExample6(client);
      runExampleHll(client);
      runExample7(client);
      runExample8(client);
      runExample9(client);
      runExample10(client);
      runExampleXDRFilter(client);

    }
    private void runExample1(AerospikeClient client) throws Exception {
        // Using binExists() expression for put or get
        System.out.println("\nRun Example 1 (binExists expression for put / get) ============");

        Key key = new Key("test", "testset", "putgetkey");
        Bin bin1 = new Bin("bin1", "v1");
        Bin bin2 = new Bin("bin2", "v2");

        client.delete(null, key);

        client.put(null, key, bin1, bin2);
        Record record = client.get(null, key);

        System.out.println("Initial Record: " + record);

        WritePolicy wPolicy = new WritePolicy(client.writePolicyDefault);

        Bin bin3 = new Bin("bin3", "v3");


        wPolicy.filterExp = Exp.build( Exp.not(Exp.binExists("bin3")));
        // first add of bin3
        client.put(wPolicy, key, bin3);
        record = client.get(null, key);
        System.out.println("Add bin3=v3 if not exist: " + record);

        // change value of bin3 to 'v3_new', try to change if not exist
        bin3 = new Bin("bin3", "v3_new");
        client.put(wPolicy, key, bin3);
        record = client.get(null, key);
        System.out.println("Add bin3=v3_new if bin does not exist(it exists, so we must fail to update): " + record);

        // update anyway
        client.put(null, key, bin3);

        record = client.get(null, key);
        System.out.println("update bin3=v3_new anyway (i.e.without using expressions): " + record);

    }


    private void runExample2(AerospikeClient client) throws Exception {
        // Set a value to a bin which will only change once (first seen)
        System.out.println("\nRun Example 2 - Using Expression Operations to write ============");

        Key key = new Key("test", "testset", "putgetkey2");
        WritePolicy wPolicy = new WritePolicy(client.writePolicyDefault);
        wPolicy.durableDelete = false;

        client.delete(wPolicy, key); //clean up

        Record record = null;

        // Bin names
        String nFirst = "binFirstUpdate"; // LUT of First time bin was updated
        String nLast = "binLastUpdate"; // LUT of latest bin update
        String nStrData = "binStrData";
        String nNumData = "binNumData";

        for(int i=0; i<5; i++) {

          // Handle attribute - set first and last LUT values
          Bin bStrData = new Bin(nStrData, "v"+i);

          Expression firstExp = Exp.build(Exp.lastUpdate());
          Expression lastExp = Exp.build(Exp.lastUpdate());

          //Exp.add() usage example
          Expression numExp =  Exp.build(Exp.add(Exp.lastUpdate(),Exp.val(123456))); 

          //Create/Update the record
          record = client.operate(wPolicy, key,
                Operation.put(bStrData), //String data bin

                //Multiple Expression Operations, each with different Expressions.
                ExpOperation.write(nNumData, numExp, ExpWriteFlags.DEFAULT),
                ExpOperation.write(nLast, lastExp, ExpWriteFlags.DEFAULT),
                ExpOperation.write(nFirst, firstExp, ExpWriteFlags.CREATE_ONLY | ExpWriteFlags.POLICY_NO_FAIL) 

                // first update, set once using CREATE_ONLY flag for Write Expression Operation.
                // On next update, CREATE_ONLY fails but POLICY_NO_FAIL ensures it does 
                // not generate an exception and abort all updates.
          );

          record = client.get(null, key);
          System.out.println("Record Create/Update #: "+i+" : " + record);
          TimeUnit.MILLISECONDS.sleep(5);  //Advance LUT by 5 milliseconds
        }
    }

    private void runExample3(AerospikeClient client) throws Exception {
        // Move value from bin to bin, overwrite old value (curr, prev)
        System.out.println("\nRun Example 3 ============");

        Key key = new Key("test", "testset", "putgetkey3");
        WritePolicy wPolicy = new WritePolicy(client.writePolicyDefault);
//        wPolicy.durableDelete = true;

        client.delete(wPolicy, key);

        Record record;

        // Bin names
        String currCellName = "currc"; // current cell tower
        String lastCellName = "prevc"; // last cell tower
        String msisdnName   = "msisdn"; // phone id

        //
        Bin msisdn = new Bin(msisdnName, "abcdef");
        Bin currCell = new Bin(currCellName, "123");

        Expression exp = Exp.build(
                Exp.stringBin(currCellName)
        );

        //noinspection UnusedAssignment
        record = client.operate(wPolicy, key,
                Operation.put(msisdn),
                ExpOperation.write(lastCellName, exp, ExpWriteFlags.EVAL_NO_FAIL),
                Operation.put(currCell));

        record = client.get(null, key);
        System.out.println("***: " + record);

        currCell = new Bin(currCellName, "456");

        //noinspection UnusedAssignment
        record = client.operate(wPolicy, key,
                Operation.put(msisdn),
                ExpOperation.write(lastCellName, exp, ExpWriteFlags.DEFAULT),
                Operation.put(currCell));

        record = client.get(null, key);
        System.out.println("###: " + record);

        currCell = new Bin(currCellName, "789");

        //noinspection UnusedAssignment
        record = client.operate(wPolicy, key,
                Operation.put(msisdn),
                ExpOperation.write(lastCellName, exp, ExpWriteFlags.DEFAULT),
                Operation.put(currCell));

        record = client.get(null, key);
        System.out.println("@@@: " + record);
    }

    private void runExample4(AerospikeClient client) throws Exception {
        // Set default value for a key if not exist, get it anyway
        System.out.println("\nRun Example 4  ============");

        Key key = new Key("test", "testset", "putgetkey4");
        WritePolicy wPolicy = new WritePolicy(client.writePolicyDefault);
//        wPolicy.durableDelete = true;

        client.delete(wPolicy, key);

        Record record;

        // Bin names
        String bin1Name = "b1";
        String bin2Name = "b2";

        // Default values
        Expression bin1Default = Exp.build(Exp.val("default value"));
        Expression bin2Default = Exp.build(Exp.val(100));

        //noinspection UnusedAssignment
        record = client.operate(wPolicy, key,
                ExpOperation.write(bin1Name, bin1Default,ExpWriteFlags.CREATE_ONLY | ExpWriteFlags.POLICY_NO_FAIL),
                ExpOperation.write(bin2Name, bin2Default,ExpWriteFlags.CREATE_ONLY | ExpWriteFlags.POLICY_NO_FAIL),
                //Operation.get()
                Operation.get(bin1Name),
                Operation.get(bin2Name)
        );
        System.out.println("***1: " + record);
        System.out.println("bin1: " + record.getList(bin1Name).get(1));
        System.out.println("bin2: " + record.getList(bin2Name).get(1));

        record = client.get(null, key);
        System.out.println("***2: " + record);

        // set non default values
        Bin bin1 = new Bin(bin1Name, "non default value");
        Bin bin2 = new Bin(bin2Name, 200);
        client.put(null, key, bin1, bin2);

        //noinspection UnusedAssignment
        record = client.operate(wPolicy, key,
                ExpOperation.write(bin1Name, bin1Default, ExpWriteFlags.CREATE_ONLY | ExpWriteFlags.POLICY_NO_FAIL),
                ExpOperation.write(bin2Name, bin2Default, ExpWriteFlags.CREATE_ONLY | ExpWriteFlags.POLICY_NO_FAIL),
                Operation.get(bin1Name),
                Operation.get(bin2Name)
        );
        System.out.println("***1: " + record);
    }

    private void runExample5(AerospikeClient client) throws Exception {
        // increment and return true if over number
        System.out.println("\nRun Example 5  ============");

        Key key = new Key("test", "testset", "incrkey4");
        WritePolicy wPolicy = new WritePolicy(client.writePolicyDefault);
//        wPolicy.durableDelete = true;

        // Bin names
        String bin1Name = "b1";
        String bin2Name = "bcnt";

        Bin bin1 = new Bin(bin1Name, "value1");
        Bin bin2 = new Bin(bin2Name, 1);

        client.delete(null, key);

        client.put(null, key, bin1, bin2);

        Record record;

        Expression rexp = Exp.build(
                Exp.gt(Exp.intBin(bin2Name),
                       Exp.val(0))
        );

        //noinspection UnusedAssignment
        record = client.operate(wPolicy, key,
                Operation.add(bin2),
                ExpOperation.read(bin2Name, rexp, ExpReadFlags.DEFAULT)
        );

        System.out.println("***1: " + record);
        System.out.println("***1b: " + record.getValue(""));


        record = client.get(null, key);
        System.out.println("***2: " + record);
    }

    private void badExample4(AerospikeClient client) throws Exception {
        Key key = new Key("test", "testset", "putgetkey3");
        WritePolicy wPolicy = new WritePolicy(client.writePolicyDefault);
//        wPolicy.durableDelete = true;

        client.delete(wPolicy, key);

        Record record;

        // Bin names
        String currCellName = "cc"; // current cell tower
        String lastCellName = "lc"; // last cell tower
        String msisdnName   = "msisdn"; // phone id

        // Handle attribute - set first seen and last seen values
        Bin msisdn = new Bin(msisdnName, "abcdef");
        Bin currCell = new Bin(currCellName, "123");

        Expression exp = Exp.build(
                Exp.def("curr", Exp.stringBin(currCellName))
        );

        Expression exp2 = Exp.build(
                Exp.var("curr")
        );

        //noinspection UnusedAssignment
        record = client.operate(wPolicy, key,
                Operation.put(msisdn),
                ExpOperation.read("currCellExp", exp, ExpReadFlags.EVAL_NO_FAIL),
                ExpOperation.write(lastCellName, exp2, ExpWriteFlags.DEFAULT),
                Operation.put(currCell));

        record = client.get(null, key);
        System.out.println("***: " + record);

        currCell = new Bin(currCellName, "456");

        //noinspection UnusedAssignment
        record = client.operate(wPolicy, key,
                Operation.put(msisdn),
                ExpOperation.write(lastCellName,exp,ExpWriteFlags.DEFAULT),
                Operation.put(currCell));

        record = client.get(null, key);
        System.out.println("###: " + record);

//        // change value, keep original first seen value
//        attr1 = new Bin(attr1Name, "value2");
//        lSeen = new Bin(lSeenName, 200);
//        fSeen = Exp.build(Exp.val(200));
//
//        //noinspection UnusedAssignment
//        record = client.operate(null, key,
//                Operation.put(attr1),
//                Operation.put(lSeen),
//                ExpOperation.write(fSeenName, fSeen, ExpWriteFlags.CREATE_ONLY | ExpWriteFlags.POLICY_NO_FAIL));
//
//        record = client.get(null, key);
//        System.out.println("###: " + record);
    }

    private void runExample6(AerospikeClient client) throws Exception {
        System.out.println("\nRun Example 6  ============");

        Key key = new Key("test", "testset", "putgetkey6");
        Bin bin1 = new Bin("bin1", "value1");
        Bin bin2 = new Bin("bin2", "value2");

        client.delete(null, key);

        client.put(null, key, bin1, bin2);
        Record record = client.get(null, key);

        System.out.println("Initial Record:                 " + record);

        WritePolicy wPolicy = new WritePolicy(client.writePolicyDefault);

        Bin bin3 = new Bin("bin3", "");
        Bin bin4 = new Bin("bin4", "value4");

        long X  = TimeUnit.DAYS.toSeconds(183);

        wPolicy.expiration = (int) X;
        wPolicy.filterExp = Exp.build(
                Exp.eq(Exp.stringBin("bin2"), Exp.val("value2")));

        client.put(wPolicy, key, bin1, bin3);
        record = client.get(null, key);
        System.out.println("Add bin3 if Bin2 is \"value2\"    " + record);

        wPolicy.filterExp = Exp.build(
                Exp.eq(Exp.stringBin("bin2"), Exp.val("value3")));

        client.put(wPolicy, key, bin1, bin4);
        record = client.get(null, key);
        System.out.println("Add bin4 if Bin2 is \"value3\"    " + record);

//        // change value to bin3, try to change if not exist
//        bin3 = new Bin("bin3", "modified value");
//        client.put(wPolicy, key, bin3);
//        record = client.get(null, key);
//        System.out.println("Add bin3 if not exist (exists): " + record);
//
//        // update anyway
//        client.put(null, key, bin3);
//
//        record = client.get(null, key);
//        System.out.println("update bin3 anyway:             " + record);
        System.out.println("Bye");
    }

    private void runExampleHll(AerospikeClient client) throws Exception {
        System.out.println("\nRun Example HLL ============");

        Key key = new Key("test", "testset", "keyHLL");
        Bin bin1 = new Bin("bin1", "value1");

        client.delete(null, key);

        Record r = client.operate(null, key,
                HLLOperation.init(HLLPolicy.Default, "BinHLL", 14, 25));
        System.out.println("init: " + r);

        HLLPolicy u = new HLLPolicy(HLLWriteFlags.UPDATE_ONLY);

        List<Value> messages = Arrays.asList(Value.get("Hello"), Value.get("Hello"), Value.get("bbb"));

        r = client.operate(null, key,
                HLLOperation.add(HLLPolicy.Default, "BinHLL", messages, 14, 25));

        System.out.println("add : " + r);

        r = client.operate(null, key,
                HLLOperation.getCount("BinHLL"));
        System.out.println("cnt : " + r);
    }

    private void runExample7(AerospikeClient client) throws Exception {
        //Get record size
        System.out.println("\nRun Example 7 ============");

        Key key = new Key("test", "testset", "putgetkey6");

        Expression rexp = Exp.build(
                 Exp.memorySize()
                // Exp.deviceSize()
//                Exp.ttl()
        );

        //noinspection UnusedAssignment
        Record record = client.operate(null, key,
                ExpOperation.read("val", rexp, ExpReadFlags.EVAL_NO_FAIL)
        );

        System.out.println("rec : " + record);

    }

    private void runExample8(AerospikeClient client) throws Exception {
        //Write optimization, don't overwrite if record is the same
        System.out.println("\nRun Example 8 ============");
        Key key = new Key("test", "testset", "putgetkey6");
        Bin bin1 = new Bin("bin1", "value1");
        Bin bin2 = new Bin("bin2", "value2");
        Bin bin1_ts = new Bin("bin1_ts", 123);

        client.delete(null, key);

        client.put(null, key, bin1,  bin1_ts, bin2);
        Record record = client.get(null, key);

        System.out.println("Initial Record:                              " + record);

        WritePolicy wPolicy = new WritePolicy(client.writePolicyDefault);

        long X  = TimeUnit.DAYS.toSeconds(183);

        wPolicy.expiration = (int) X;
        wPolicy.filterExp = Exp.build(
                Exp.and(
                        Exp.or(Exp.not(Exp.binExists("bin1")), Exp.ne(Exp.stringBin("bin1"), Exp.val("value1"))),
                        Exp.or(Exp.not(Exp.binExists("bin1_ts")), Exp.ne(Exp.intBin("bin1_ts"), Exp.val(123))),
                        Exp.or(Exp.not(Exp.binExists("bin2")), Exp.ne(Exp.stringBin("bin2"), Exp.val("value2")))
                ));

        client.put(wPolicy, key, bin1, bin1_ts, bin2);
        record = client.get(null, key);
        System.out.println("Don't update if record is the same (basic)   " + record);

        bin1 = new Bin("bin1", "value11");
        bin1_ts = new Bin("bin1_ts", 456);

        wPolicy.filterExp = Exp.build(
                    Exp.or(Exp.not(Exp.binExists("bin1")), Exp.ne(Exp.stringBin("bin1"), Exp.val("value11")))
                );

        client.put(wPolicy, key, bin1, bin1_ts);
        record = client.get(null, key);
        System.out.println("Update record if bin1 was not \"value11\"      " + record);

        bin1_ts = new Bin("bin1_ts", 789);
        client.put(wPolicy, key, bin1, bin1_ts);
        record = client.get(null, key);
        System.out.println("Don't update record if bin1 was \"value11\"  diff bin1_ts   " + record);

        Bin bin3 = new Bin("bin3", "value3");

        wPolicy.filterExp = Exp.build(
                Exp.or(Exp.not(Exp.binExists("bin3")), Exp.ne(Exp.stringBin("bin3"), Exp.val("value3"))));

        client.put(wPolicy, key, bin3);
        record = client.get(null, key);
        System.out.println("Add bin3 if not exist    " + record);

        System.out.println("Bye");
    }

    private void runExample9(AerospikeClient client) throws Exception {
        System.out.println("\nRun Example 9 ============");

        Key key = new Key("test", "testset", "putgetkey9");
        WritePolicy wPolicy = new WritePolicy(client.writePolicyDefault);

        client.delete(null, key);

        wPolicy.filterExp = Exp.build(
                Exp.or(Exp.not(Exp.binExists("bin1")), Exp.lt(Exp.val(123), Exp.intBin("bin1")))
        );

        Bin bin1 = new Bin("bin1",123);

        client.put(wPolicy, key, bin1);
        Record record = client.get(null, key);
        System.out.println("record: " + record);

        bin1 = new Bin("bin1",456);
        wPolicy.filterExp = Exp.build(
                Exp.or(Exp.not(Exp.binExists("bin1")), Exp.lt(Exp.val(456), Exp.intBin("bin1")))
        );
        client.put(wPolicy, key, bin1);
        record = client.get(null, key);
        System.out.println("record: " + record);

        bin1 = new Bin("bin1",111);
        wPolicy.filterExp = Exp.build(
                Exp.or(Exp.not(Exp.binExists("bin1")), Exp.lt(Exp.val(111), Exp.intBin("bin1")))
        );
        client.put(wPolicy, key, bin1);
        record = client.get(null, key);
        System.out.println("record: " + record);



    }

    private void runExample10(AerospikeClient client) throws Exception {
        // Get LUT of a record
        System.out.println("\nRun Example 10 ============");

        Key key = new Key("test", "testset", "putgetkey10");
        WritePolicy wPolicy = new WritePolicy(client.writePolicyDefault);

        client.delete(null, key);

        Bin bin1 = new Bin("bin1",123);
        Bin bin2 = new Bin("bin2", true);

        wPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        wPolicy.expiration = 60 * 86400;

        client.put(wPolicy, key, bin1, bin2);
        Record record = client.get(null, key);
        System.out.println("record: " + record);

        //        boolean isExists = client.exists(null, key);

        Expression exp = Exp.build(
                Exp.lastUpdate());

        Record record2 = client.operate(wPolicy, key,
                ExpOperation.read("", exp, ExpReadFlags.DEFAULT)
        );
        System.out.println("record2: " + record2);

    }



    private void runExampleXDRFilter(AerospikeClient client) throws Exception {
        System.out.println("\nRun XDR Filter Example ============");
        System.out.println("base64 string if loading XDR filter via asinfo: ");  
        Expression filter1 = Exp.build( Exp.or(
                                        Exp.isTombstone(),   //OR Tombstone - allows shipping record deletes
                                        Exp.not(Exp.binExists("a")) 
                                       ) 
                                     );
        System.out.println("  isTombsone || bin \"a\" does not exist: "+filter1.getBase64());  
        
        //Record metadata filter to ship live-for-ever (ttl=-1) records only
        Expression filter2 = Exp.build(Exp.lt(Exp.ttl(), Exp.val(0)));
        System.out.println("  live-for-ever records only: "+filter2.getBase64());  

        //Record metadata filter to ship records that are > 127K size on device.
        Expression filter3 = Exp.build(Exp.ge(Exp.deviceSize(), Exp.val(127 * 1024))); 
        System.out.println("  Records>127KB on device: "+filter3.getBase64());  

        //Registering XDR filter on all server nodes via client
        WritePolicy wp = client.writePolicyDefault;
        Expression filter = null;
        client.setXDRFilter(client.infoPolicyDefault, "DC1","test", filter3);

        //setXDRFilter: filter=null to clear the filter
    }

//Other Filter examples ====================
/*
        Expression filter = Exp.build(Exp.ge(Exp.intBin("age"), Exp.val(21)));
        System.out.println(filter.getBase64());
        client.setXDRFilter(client.infoPolicyDefault, "kafkaDC","test", filter);

        wPolicy.filterExp = Exp.build(
                              Exp.or(
                                Exp.and(
                                  Exp.gt(Exp.intBin("firstSeen"), Exp.val(126)),
                                  Exp.le(Exp.intBin("lastSeen"), Exp.val(140))
                                ),
                                Exp.eq(Exp.stringBin("override"), Exp.val("YES"))
                              )
                            );

        wPolicy.filterExp = Exp.build(
                              Exp.and(
                                (Exp.gt(Exp.intBin("lastSeen"), Exp.val(1234))),
                                (Exp.bin)
                              )
                            );
*/
//Resume ===========
}
