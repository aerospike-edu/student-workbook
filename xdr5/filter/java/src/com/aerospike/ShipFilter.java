package com.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;

public class ShipFilter {

  public static void main(String[] args) {
    AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);

    Expression filter = Exp.build(Exp.or(Exp.isTombstone(), Exp.ge(Exp.intBin("age"), Exp.val(21))));

    try {
          client.setXDRFilter(null, "DC1", "ns1", filter); //Info Policy, datacenter, namespace, filter object
        } catch (AerospikeException e) {
          System.out.println("Failed to set filter " + e.getMessage());
        }

    client.close();
  }
}
