package com.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.Value;
import com.aerospike.client.exp.*;
import com.aerospike.client.exp.HLLExp;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.operation.HLLPolicy;
import com.aerospike.client.operation.HLLWriteFlags;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.operation.HLLOperation;
import java.util.Arrays;
import java.util.List;


public class HLLExpression {
    public static void main(String[] args) throws Exception {
      HLLExpression ce = new HLLExpression();
      ce.runExample();
      return;
    }

    public void runExample() throws Exception {
      AerospikeClient client = new AerospikeClient("127.0.0.1", 3000); //Update IP Address
      runExampleHLL(client);
    }
    private void runExampleHLL(AerospikeClient client) throws Exception {
        System.out.println("\nRun Example HLL ============");

        Key key = new Key("test", "testset", "keyHLL");
        client.delete(null, key);

        List<Value> urls = Arrays.asList( 
                Value.get("abc.com"), 
                Value.get("cde.com"), 
                Value.get("abc.com"), 
                Value.get("abc.com"), 
                Value.get("xyz.com"),
                Value.get("pqr.com"),
                Value.get("abc.com"), 
                Value.get("abc.com"), //1
                Value.get("abc.com"), 
                Value.get("cde.com"), 
                Value.get("abc.com"), 
                Value.get("abc.com"), 
                Value.get("xyz.com"),
                Value.get("pqr.com"),
                Value.get("abc.com"), 
                Value.get("abc.com"), //2
                Value.get("abc.com"), 
                Value.get("cde.com"), 
                Value.get("abc.com"), 
                Value.get("abc.com"), 
                Value.get("xyz.com"),
                Value.get("pqr.com"),
                Value.get("abc.com"), 
                Value.get("abc.com"), //3
                Value.get("abc.com"), 
                Value.get("cde.com"), 
                Value.get("abc.com"), 
                Value.get("abc.com"), 
                Value.get("xyz.com"),
                Value.get("pqr.com"),
                Value.get("abc.com"), 
                Value.get("abc.com")  //4
                );

        Bin urlbin = new Bin("urls", urls);

        client.put(null, key, urlbin);

        Record r = client.get(null,key);

        System.out.println("record: " + r);

        HLLPolicy hllPolicy = new HLLPolicy(HLLWriteFlags.DEFAULT);
        Expression rexpw = Exp.build(
            HLLExp.add(hllPolicy, 
                       ListExp.getByIndexRange(ListReturnType.VALUE, Exp.val(0), Exp.listBin("urls")), 
                       Exp.hllBin("urlshll") 
            )
        );

        Expression rexpr = Exp.build(HLLExp.getCount(Exp.hllBin("urlshll")));

        Expression rexpd = Exp.build(Exp.nil());

        r = client.operate(null, key,
                ListOperation.size("urls"),
                HLLOperation.init(hllPolicy, "urlshll", 4, 0),
                ExpOperation.write("urlshll", rexpw, ExpWriteFlags.DEFAULT),
                ExpOperation.read("retval", rexpr, ExpReadFlags.DEFAULT),
                Operation.put(new Bin("urlshll", Value.NULL))
        );

        System.out.println("Cardinality: " + r);
    }
}
