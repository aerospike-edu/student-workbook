package com.aerospike;

import com.aerospike.client.*;
import com.aerospike.documentapi.AerospikeDocumentClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.aerospike.documentapi.JsonConverters;

public class DocApiIntro {
    public static void main(String[] args) throws Exception {
      DocApiIntro ce = new DocApiIntro();
      ce.runExample();
      return;
    }

    public void runExample() throws Exception {
      AerospikeClient client = new AerospikeClient("127.0.0.1", 3000); //Update IP Address
      AerospikeDocumentClient docClient = new AerospikeDocumentClient(client);
      runDocApiExample1(docClient);
    }

    private void runDocApiExample1(AerospikeDocumentClient docClient) throws Exception {
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
    }
}
