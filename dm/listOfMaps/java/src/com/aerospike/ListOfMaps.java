package com.aerospike;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.ListSortFlags;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.cdt.ListPolicy;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.cdt.MapReturnType;

public class ListOfMaps {
	public static void appendToList(AerospikeClient client, Map<Value, Value> mvalue) {
		Key key = new Key("test", "testList", "k1");
		client.operate(null, key, ListOperation.append(ListPolicy.Default, "myList", Value.get(mvalue)));
	}

	public static void main(String[] args) {
		AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);		

		Key key = new Key("test", "testList", "k1");
		WritePolicy policy = new WritePolicy();
		policy.recordExistsAction = RecordExistsAction.REPLACE;
		client.delete(policy, key);

                Map<Value, Value> m1 = new HashMap<Value, Value>();
                m1.put(Value.get("cv1"), Value.get(11));
                m1.put(Value.get("cv2"), Value.get(12));
                m1.put(Value.get("cv3"), Value.get(13));
                m1.put(Value.get("cv4"), Value.get(14));
                m1.put(Value.get("cv5"), Value.get(15));

		appendToList(client, m1);
                Map<Value, Value> m2 = new HashMap<Value, Value>();
                m2.put(Value.get(1), Value.get(11));
                m2.put(Value.get(2), Value.get(12));
                m2.put(Value.get(3), Value.get(13));
                m2.put(Value.get(4), Value.get(14));
                m2.put(Value.get(5), Value.get(15));

		appendToList(client, m2);
		
		System.out.println("getByIndexRange(0,2) = " + 
		client.operate(null, key, ListOperation.getByIndexRange("myList",0,2,ListReturnType.VALUE)));
		
		//System.out.println("getByValue(3) = " + 
		//client.operate(null, key, ListOperation.getByValue("myList",Value.get(3),ListReturnType.INDEX)));

		//List<Value> compList = new ArrayList<Value>();
		//compList.add(Value.get(2));
		//compList.add(Value.get(3));
		//compList.add(Value.get(8));

        //System.out.println("getByValueList([2,3,8]) = " + 
        //client.operate(null, key, ListOperation.getByValueList("myList",compList,ListReturnType.INDEX)));
		client.close();
	}
}





