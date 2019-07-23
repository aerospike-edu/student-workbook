package com.aerospike;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.cdt.ListPolicy;

public class ListTimeSeries {
	public static void appendToList(AerospikeClient client, List<Value> value) {
		Key key = new Key("test", "testList", "k1");
		client.operate(null, key, ListOperation.append(ListPolicy.Default, "myList", Value.get(value)));
		}

	public static void main(String[] args) {
		AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);
		
		Key key = new Key("test", "testList", "k1");
		WritePolicy policy = new WritePolicy();
		policy.recordExistsAction = RecordExistsAction.REPLACE;
		
		client.delete(policy, key);
		
		List<Value> botList = new ArrayList<Value>();
		botList.add(Value.get(102)); 
		//botList.add(Value.get("ab")); //Also works with strings
		//botList.add(Value.get(0)); // 0 = nil also works instead on Value.WILDCARD
		botList.add(Value.WILDCARD);
		
		List<Value> topList = new ArrayList<Value>();
		topList.add(Value.get(104));
		//topList.add(Value.get("ab"+1));  //Also works with strings
		//topList.add(Value.get(0));  // 0 = nil also works instead on Value.WILDCARD
		botList.add(Value.WILDCARD);
		
		//List<Value> l1 = new ArrayList<Value>();
		//l1.add(Value.StringValue.get("abc"));
		//l1.add(Value.StringValue.get("bde"));    
		
		Map<Value, Value> m1 = new HashMap<Value, Value>();
		
		//t=101
		m1.put(Value.get("k1"), Value.get("blah1"));
		m1.put(Value.get("k2"), Value.get("foo1"));
		m1.put(Value.get("k3"), Value.get("bar1")); 
		
		List<Value> inList1 = new ArrayList<Value>();
		inList1.add(Value.get(101));
		inList1.add(Value.MapValue.get(m1));
		appendToList(client, inList1);
		
		//t=102
		m1.put(Value.get("k1"), Value.get("blah2"));
		m1.put(Value.get("k2"), Value.get("foo2"));
		m1.put(Value.get("k3"), Value.get("bar2"));
		
		List<Value> inList2 = new ArrayList<Value>();
		inList2.add(Value.get(102));
		inList2.add(Value.MapValue.get(m1));
		appendToList(client, inList2);
		
		//t=103
		m1.put(Value.get("k1"), Value.get("blah3"));
		m1.put(Value.get("k2"), Value.get("foo3"));
		m1.put(Value.get("k3"), Value.get("bar3"));
		
		List<Value> inList3 = new ArrayList<Value>();
		inList3.add(Value.get(103));
		inList3.add(Value.MapValue.get(m1));
		appendToList(client, inList3);
		
		//t=104
		m1.put(Value.get("k1"), Value.get("blah4"));
		m1.put(Value.get("k2"), Value.get("foo4"));
		m1.put(Value.get("k3"), Value.get("bar4"));
		
		List<Value> inList4 = new ArrayList<Value>();
		inList4.add(Value.get(104));
		inList4.add(Value.MapValue.get(m1));
		appendToList(client, inList4);
		
		//t=105
		m1.put(Value.get("k1"), Value.get("blah5"));
		m1.put(Value.get("k2"), Value.get("foo5"));
		m1.put(Value.get("k3"), Value.get("bar5"));
				
		List<Value> inList5 = new ArrayList<Value>();
		inList5.add(Value.get(105));
		inList5.add(Value.MapValue.get(m1));
		appendToList(client, inList5);
		
		System.out.println("getByIndexRange(0,5) = " + 
		client.operate(null, key, ListOperation.getByIndexRange("myList",0,5,ListReturnType.VALUE)));
		
		System.out.println("getByValueRange([102,*] thru [104,*]) = " + 
		client.operate(null, key, 
				ListOperation.getByValueRange("myList",
						Value.ListValue.get(botList),
						Value.ListValue.get(topList), 
						ListReturnType.VALUE)));
		client.close();
		}
	}
