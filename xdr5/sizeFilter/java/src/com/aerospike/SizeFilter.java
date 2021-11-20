package com.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;

public class SizeFilter {

  public static void main(String[] args) {
    AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);

    Expression filter = Exp.build( Exp.ge(Exp.deviceSize(), Exp.val(127 * 1024)) );
    String sFilter = filter.getBase64();
    System.out.println("Filter in base64:"+ sFilter); //Prints: kwSRQc4AAfwA

    client.close();
  }
}

/*
Set filter on any node, will be set on all via SMD.

$ asinfo -v "xdr-set-filter:dc=DC1;namespace=ns1;exp=kwSRQc4AAfwA"'
ok

Check the filter is there:

$ asinfo -v "xdr-get-filter:dc=DC1;namespace=ns1"
namespace=shared:exp=ge(device_size(), 130048)

Delete the filter:
$ asinfo -v "xdr-set-filter:dc=DC1;namespace=ns1;exp=null"'
ok
*/
