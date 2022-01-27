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
                Exp.val(1) 
              )
            );
        rPolicy.failOnFilteredOut = false;
        obj = docClient.get(rPolicy, doc1, "docBin", jsonPath);  //Using Read Filter Expression 
        if(obj != null) {
          System.out.println("docClient Read Filter Expression (false): "+obj.toString());
        } else {
          System.out.println("docClient Read Filter Expression (false) returned null object");
        }
       
        Record r = client.get(rPolicy, doc1);  //Using Read Filter Expression
        System.out.println("client Read Filter Expression (false): "+ r);
    }
}
