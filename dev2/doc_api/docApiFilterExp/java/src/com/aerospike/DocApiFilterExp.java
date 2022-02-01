package com.aerospike;

import com.aerospike.client.*;
import com.aerospike.documentapi.AerospikeDocumentClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.aerospike.documentapi.JsonConverters;

import com.aerospike.client.exp.*;
import com.aerospike.client.exp.Exp.Type;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListReturnType;

import com.aerospike.client.policy.WritePolicy;

public class DocApiFilterExp {
    public static void main(String[] args) throws Exception {
      DocApiFilterExp ce = new DocApiFilterExp();
      ce.runExample();
      return;
    }

    public void runExample() throws Exception {
      AerospikeClient client = new AerospikeClient("127.0.0.1", 3000); //Update IP Address
      AerospikeDocumentClient docClient = new AerospikeDocumentClient(client);
      runDocApiExample1(docClient, client);
    }

    private void runDocApiExample1(AerospikeDocumentClient docClient, AerospikeClient client) throws Exception {
        System.out.println("Insert a JSON Document in a Record Bin");
        String jsonString = "{\"k1\":\"v1\", \"k2\":[1,2,3], \"k3\":[\"v31\", \"v32\", \"v34\"]}";

        //Convert json string to json node
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(jsonString);


        //Record Key object
        Key doc1 = new Key("test","docset","doc1");
        client.delete(null, doc1); //Housekeeping

        //Insert document to DB
        docClient.put(doc1, "docBin", jsonNode);  //Using default WritePolicy


        System.out.println( docClient.get(doc1,"docBin","$") ); //Print inserted object

        //What was that ^ ?? Explained below:

        System.out.println("\nRead list item at index 1 for map value at key=k3");
        //Read the document specified by jsonpath in a given bin
        String jsonPath = "$.k3[1]" ; //Expect "v32"
        Object obj = docClient.get(doc1, "docBin", jsonPath);  //Using default ReadPolicy
        System.out.println(obj.toString());


        //Using Read Filter Expression 
        Policy rPolicy = new Policy();

        rPolicy.filterExp = Exp.build( 
              Exp.gt( 
                ListExp.getByIndex(
                    ListReturnType.VALUE, 
                    Exp.Type.INT, 
                    Exp.val(0), 
                    Exp.mapBin("docBin"), 
                    CTX.mapKey(Value.get("k2")) ),
                Exp.val(2) 
              )
            );
        rPolicy.failOnFilteredOut = false;  //Default is false 

        obj = docClient.get(rPolicy, doc1, "docBin", jsonPath);  //Using Read Filter Expression 
        if(obj != null) {
          System.out.println("\nDocument Client Read Filter Expression [k2[0]>2] (false): "+obj.toString());
        } else {
          System.out.println("\nDocument Client Read Filter Expression [k2[0]>2] (false) returned null object");
        }
       
        Record r = client.get(rPolicy, doc1);  //Using Read Filter Expression
        System.out.println("AerospikeClient Read Filter Expression [k2[0]>2] (false): "+ r);

        //Read filter true
        rPolicy.filterExp = Exp.build( 
              Exp.gt( 
                ListExp.getByIndex(
                    ListReturnType.VALUE, 
                    Exp.Type.INT, 
                    Exp.val(0), 
                    Exp.mapBin("docBin"), 
                    CTX.mapKey(Value.get("k2")) ),
                Exp.val(0) 
              )
            );
        rPolicy.failOnFilteredOut = false;
        obj = docClient.get(rPolicy, doc1, "docBin", jsonPath);  //Using Read Filter Expression 
        if(obj != null) {
          System.out.println("\nDocument Client Read Filter Expression [k2[0]>0] (true): "+obj.toString());
        } else {
          System.out.println("\nDocument Client Read Filter Expression [k2[0]>0] (true) returned null object");
        }
       
        r = client.get(rPolicy, doc1);  //Using Read Filter Expression
        System.out.println("AerospikeClient Read Filter Expression [k2[0]>0 (true): "+ r);

        long lut_01_01_2022 = 1641027600000000000L;
        long lut_01_01_2032 = 1956560400000000000L;

        //Read filter false on record metadata
        rPolicy.filterExp = Exp.build( Exp.gt(Exp.lastUpdate(), Exp.val(lut_01_01_2032)));

        obj = docClient.get(rPolicy, doc1, "docBin", jsonPath);  //Using Read Filter Expression 
        if(obj != null) {
          System.out.println("\nDocument Client Read Filter Expression [lut>1/1/32] (false): "+obj.toString());
        } else {
          System.out.println("\nDocument Client Read Filter Expression [lut>1/1/32] (false) returned null object");
        }
       
        r = client.get(rPolicy, doc1);  //Using Read Filter Expression
        System.out.println("AerospikeClient Read Filter Expression [lut>1/1/32] (false): "+ r);

        //Read filter true on record metadata

        rPolicy.filterExp = Exp.build( Exp.gt(Exp.lastUpdate(), Exp.val(lut_01_01_2022)));

        obj = docClient.get(rPolicy, doc1, "docBin", jsonPath);  //Using Read Filter Expression 
        if(obj != null) {
          System.out.println("\nDocument Client Read Filter Expression [lut>1/1/22] (true): "+obj.toString());
        } else {
          System.out.println("\nDocument Client Read Filter Expression [lut>1/1/22] (true) returned null object");
        }
       
        r = client.get(rPolicy, doc1);  //Using Read Filter Expression
        System.out.println("AerospikeClient Read Filter Expression [lut>1/1/22] (true): "+ r);

        //Write Filter Expression - insert another bin with jsonNode
        //with record data and record metadata filter

        WritePolicy wPolicy = new WritePolicy();
        //Write filter false 
        wPolicy.filterExp = Exp.build( 
              Exp.gt( 
                ListExp.getByIndex(
                    ListReturnType.VALUE, 
                    Exp.Type.INT, 
                    Exp.val(0), 
                    Exp.mapBin("docBin"), 
                    CTX.mapKey(Value.get("k2")) ),
                Exp.val(2) 
              )
            );
        docClient.put(wPolicy, doc1, "docRDataBin", jsonNode);  //Using Write Filter Expression 
        r = client.get(null, doc1); 
        System.out.println("\nDocument API with Write Filter Expression [k2[0]>2] (false): "+ r);

        //Write filter true
        wPolicy.filterExp = Exp.build( 
              Exp.gt( 
                ListExp.getByIndex(
                    ListReturnType.VALUE, 
                    Exp.Type.INT, 
                    Exp.val(0), 
                    Exp.mapBin("docBin"), 
                    CTX.mapKey(Value.get("k2")) ),
                Exp.val(0) 
              )
            );
        docClient.put(wPolicy, doc1, "docRDataBin", jsonNode);  //Using Write Filter Expression 
        r = client.get(null, doc1); 
        System.out.println("Document API with Write Filter Expression [k2[0]>0 (true): "+ r);

        //Write filter false on record metadata
        wPolicy.filterExp = Exp.build( Exp.gt(Exp.lastUpdate(), Exp.val(lut_01_01_2032)));

        docClient.put(wPolicy, doc1, "docRMetaBin", jsonNode);  //Using Write Filter Expression 
        r = client.get(null, doc1); //Read the record 
        System.out.println("\nDocument API with Write Filter Expression [lut>1/1/32] (false): "+ r);

        //Write filter true on record metadata
        wPolicy.filterExp = Exp.build( Exp.gt(Exp.lastUpdate(), Exp.val(lut_01_01_2022)));

        docClient.put(wPolicy, doc1, "docRMetaBin", jsonNode);  //Using Write Filter Expression 
        r = client.get(null, doc1);  //Read the record
        System.out.println("Document API with Write Filter Expression [lut>1/1/22] (true): "+ r);

        //Check append with Filter Expression.
        wPolicy.filterExp = Exp.build( Exp.gt(Exp.lastUpdate(), Exp.val(lut_01_01_2032)));
        docClient.append(wPolicy, doc1, "docRMetaBin", "$.k3", "v35");  //Using Write Filter Expression 
        r = client.get(null, doc1);  //Read the record
        System.out.println("\nDocument API append v35 to [$.k3] with Write Filter Expression [lut>1/1/32] (false): "+ r);

        wPolicy.filterExp = Exp.build( Exp.gt(Exp.lastUpdate(), Exp.val(lut_01_01_2022)));
        docClient.append(wPolicy, doc1, "docRMetaBin", "$.k3", "v35");  //Using Write Filter Expression 
        r = client.get(null, doc1);  //Read the record
        System.out.println("Document API append v35 to [$.k3] with Write Filter Expression [lut>1/1/22] (true): "+ r);


        //Check delete with Filter Expression.
        wPolicy.filterExp = Exp.build( Exp.gt(Exp.lastUpdate(), Exp.val(lut_01_01_2032)));
        docClient.delete(wPolicy, doc1, "docRMetaBin", "$.k3");  //Using Write Filter Expression 
        r = client.get(null, doc1);  //Read the record
        System.out.println("\nDocument API delete [$.k3] with Write Filter Expression [lut>1/1/32] (false): "+ r);

        wPolicy.filterExp = Exp.build( Exp.gt(Exp.lastUpdate(), Exp.val(lut_01_01_2022)));
        docClient.delete(wPolicy, doc1, "docRMetaBin", "$.k3");  //Using Write Filter Expression 
        r = client.get(null, doc1);  //Read the record
        System.out.println("Document API delete [$.k3] with Write Filter Expression [lut>1/1/22] (true): "+ r);
    }
}
