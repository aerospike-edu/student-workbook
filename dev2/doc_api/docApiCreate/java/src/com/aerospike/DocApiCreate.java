package com.aerospike;

import com.aerospike.client.*;
import com.aerospike.documentapi.AerospikeDocumentClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.aerospike.documentapi.JsonConverters;
import com.aerospike.client.policy.WritePolicy;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Vector;

public class DocApiCreate {
    public static void main(String[] args) throws Exception {
      DocApiCreate ce = new DocApiCreate();
      ce.runExample();
      return;
    }

    public void runExample() throws Exception {
      AerospikeClient client = new AerospikeClient("127.0.0.1", 3000); //Update IP Address
      client.delete(null, new Key("test", "docset", "doc1")); //Housekeeping

      AerospikeDocumentClient docClient = new AerospikeDocumentClient(client);
      runDocApiExample1(docClient, client);
    }

    private void runDocApiExample1(AerospikeDocumentClient docClient, AerospikeClient client) throws Exception {
        String jsonString = "{\"k1\":\"v1\", \"k2\":[1,2,3], \"k3\":[\"v31\", \"v32\", \"v34\"]}";
        System.out.println("Insert a Record with intBin, stringBin & docBin with JSON Document:"+jsonString);

        //Convert json string to json node, convert json node to json map.
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(jsonString);
        Map jsonMap = JsonConverters.convertJsonNodeToMap(jsonNode);


        //Record Key object
        Key doc1 = new Key("test","docset","doc1");

        Bin[] bins = new Bin[3];
        bins[0] = new Bin("intBin",1);
        bins[1] = new Bin("stringBin","stringValue");
        bins[2] = new Bin("docBin",jsonMap);

        WritePolicy wPolicy = new WritePolicy();
 
        //Insert document to DB
        client.put(wPolicy, doc1, bins); 

        Record r = client.get(null, doc1);
        System.out.println("\nAerospikeClient get() record =" + r);
        System.out.println( "Document Client get() docBin = "+docClient.get(doc1,"docBin","$") ); //Print inserted object
        
        String jsonPath = "$.k4";
        List<Integer> listToAdd = new Vector<>();
        listToAdd.add(1);
        listToAdd.add(2);
        listToAdd.add(3);
        docClient.put(doc1, "docBin", jsonPath, listToAdd);

        r = client.get(null, doc1);
        System.out.println("\nAdd k4 via docClient: AerospikeClient get() record =" + r);
        System.out.println( "Add k4 via docClient: Document Client get() docBin = "+docClient.get(doc1,"docBin","$") ); //Print inserted object

    }
}
