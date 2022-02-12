package com.aerospike;

import com.aerospike.client.*;
import com.aerospike.documentapi.AerospikeDocumentClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.aerospike.documentapi.JsonConverters;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Vector;

public class DocApiIntro {
    public static void main(String[] args) throws Exception {
      DocApiIntro ce = new DocApiIntro();
      ce.runExample();
      return;
    }

    public void runExample() throws Exception {
      AerospikeClient client = new AerospikeClient("127.0.0.1", 3000); //Update IP Address
      client.delete(null, new Key("test", "docset", "doc1")); //Housekeeping

      AerospikeDocumentClient docClient = new AerospikeDocumentClient(client);
      runDocApiExample1(docClient);
    }

    private void runDocApiExample1(AerospikeDocumentClient docClient) throws Exception {
        System.out.println("Insert a JSON Document in a Record Bin:");
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
        System.out.println("k3[1] = "+obj.toString());


        //Update (put) "v34" to json object "v33" as a String at index 2 in list in  k3
        jsonPath = "$.k3[2]" ; 
        docClient.put(doc1, "docBin", jsonPath, "v33"); //Use put()
        System.out.println( "\nUpdate v34 at k3[2] to v33 :\n"+docClient.get(doc1,"docBin","$") ); 

        //Append json object v35 as a String in list in  k3
        jsonPath = "$.k3" ; 
        docClient.append(doc1, "docBin", jsonPath, "v35"); //append inserts at index -1
        System.out.println( "\nAppend (at index = -1) v35 :\n"+docClient.get(doc1,"docBin","$") ); 

        //Delete json object v32 as a String at index 1 in list in  k3
        jsonPath = "$.k3[1]" ; 
        docClient.delete(doc1, "docBin", jsonPath);
        System.out.println( "\nDelete at $.k3[1] - v32 :\n"+docClient.get(doc1,"docBin","$") ); 

        //Insert a fragment as new key
        jsonPath = "$.k3" ; //Expect a list object
        obj = docClient.get(doc1, "docBin", jsonPath); 
        jsonPath = "$.k4" ; //Add json Object as value of new map key = k4
        docClient.put(doc1, "docBin", jsonPath, obj); 
        System.out.println( "\nAdd k3 as k4 :\n"+docClient.get(doc1,"docBin","$") ); 

        //Insert a Nested json object
        jsonString = "{\"x1\":[1,2,3,4,5]}";
        jsonNode = JsonConverters.convertStringToJsonNode(jsonString);
        Key tempdoc = new Key("test", "docset", "tempdoc");
        docClient.put(tempdoc, "tempBin", jsonNode);
        jsonPath = "$" ; //Expect a nested map object
        obj = docClient.get(tempdoc, "tempBin", jsonPath);
        jsonPath = "$.k5" ; //Add json Object as value of new map key = k4
        docClient.put(doc1, "docBin", jsonPath, obj); 
        System.out.println( "\nInsert new map key k5:{x1:[1,2,3,4,5]}:\n"+docClient.get(doc1,"docBin","$") ); 

        //Direct map object insertion (jsonNode insertion will corrupt the record)
        Map<String, List<Integer>> a = new HashMap<>();
        List<Integer> jsonStringList = new Vector<>();
        jsonStringList.add(1);
        jsonStringList.add(2);
        jsonStringList.add(3);
        jsonStringList.add(4);
        jsonStringList.add(5);
        a.put("x1", jsonStringList);
        docClient.put(doc1, "docBin", "$.k6", a);
        System.out.println( "\nInsert Map/List object at new map key k6:{x1:[1,2,3,4,5]}:\n"+docClient.get(doc1,"docBin","$") ); 

        //What should not be done ... 
        jsonString = "{\"x1\":[1,2,3,4,5]}";
        jsonNode = JsonConverters.convertStringToJsonNode(jsonString);
        Map jsonMap = JsonConverters.convertJsonNodeToMap(jsonNode);
        docClient.put(doc1, "docBin", "$.k7", jsonNode); //<-- DON'T DO THIS - WILL CORRUPT THE RECORD
        //May look "good" here but cross check with aql 
        System.out.println( "\nInsert new map key k7:{x1:[1,2,3,4,5]}:\n"+docClient.get(doc1,"docBin","$") ); 
        //Instead insert the jsonMap
        docClient.put(doc1, "docBin", "$.k8", jsonMap); //<-- Use jsonMap
        //Must be "good", cross check with aql 
        System.out.println( "\nInsert new map key k8:{x1:[1,2,3,4,5]}:\n"+docClient.get(doc1,"docBin","$") ); 

        //JSONPath queries
        //jsonPath = "$.k3[*]";
        //System.out.println(docClient.get(doc1, "docBin", jsonPath));
    }
}
