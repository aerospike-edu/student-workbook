/* 
 * Copyright 2012-2015 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.aerospike.developer.training;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.lua.LuaConfig;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;

public class UserService {
	private AerospikeClient client;
	private EclipseConsole console = new EclipseConsole();

	public UserService(AerospikeClient client) {
		this.client = client;
	}

	public void createUser() throws AerospikeException {
		console.printf("\n********** Create User **********\n");

	    ///*********************///
	    ///*****Data Model*****///
	    //Namespace: test
	    //Set: users
	        //Key: <username>
	        //Bins:
	            //username - String
	            //password - String (For simplicity password is stored in plain-text)
	            //gender - String (Valid values are 'm' or 'f')
	            //region - String (Valid values are: 'n' (North), 's' (South), 'e' (East), 'w' (West) -- to keep data entry to minimal we just store the first letter)
	            //lasttweeted - int (Stores epoch timestamp of the last/most recent tweet) -- Default to 0
	            //tweetcount - int (Stores total number of tweets for the user) -- Default to 0
	            //interests - Array of interests
	
		    //Sample Key: dash
		    //Sample Record:
		        //{ username: 'dash',
		        //  password: 'dash',
		        //  gender: 'm',
		        //  region: 'w',
		        //  lasttweeted: 1408574221,
		        //  tweetcount: 20,
		        //  interests: ['photography', 'technology', 'dancing', 'house music] 
		        //}
	    ///*********************///

		String username;
		String password;
		String gender;
		String region;
		String interests;

		// Get username
		console.printf("Enter username: ");
		username = console.readLine();

		if (username != null && username.length() > 0) {
			// Get password
			console.printf("Enter password for " + username + ":");
			password = console.readLine();

			// Get gender
			console.printf("Select gender (f or m) for " + username + ":");
			gender = console.readLine().substring(0, 1);

			// Get region
			console.printf("Select region (north, south, east or west) for "
					+ username + ":");
			region = console.readLine().substring(0, 1);

            // Get interests
			console.printf("Enter comma-separated interests for " + username + ":");
            interests = console.readLine();

			// Write record
            // TODO: Create WritePolicy instance
            // Exercise K2
			WritePolicy wPolicy = new WritePolicy();
			wPolicy.recordExistsAction = RecordExistsAction.UPDATE;

			// TODO: Create Key and Bin instances for the user record. 
			// Remember to convert comma-separated interests into a list before storing it.
		    // Exercise K2
			Key key = new Key("test", "users", username);
			Bin bin1 = new Bin("username", username);
			Bin bin2 = new Bin("password", password);
			Bin bin3 = new Bin("gender", gender);
			Bin bin4 = new Bin("region", region);
			Bin bin5 = new Bin("lasttweeted", 0);
			Bin bin6 = new Bin("tweetcount", 0);
			Bin bin7 = new Bin("interests", Arrays.asList(interests.split(",")));

			// TODO: Write user record
		    // Exercise K2
			client.put(wPolicy, key, bin1, bin2, bin3, bin4, bin5, bin6, bin7);

			console.printf("\nINFO: User record created!");
		}
	} //createUser
	
	public void getUser() throws AerospikeException {
		Record userRecord = null;
		Key userKey = null;

		// Get username
		String username;
		console.printf("\nEnter username:");
		username = console.readLine();

		if (username != null && username.length() > 0) {
			// TODO: Read user record
		    // Exercise K2			
			userKey = new Key("test", "users", username);
			userRecord = client.get(null, userKey);
			
			// Check if userRecord exists
			if (userRecord != null) {
				console.printf("\nINFO: User record read successfully! Here are the details:\n");
				// TODO: Output user record to the console. 
				// Remember to convert list into comma-separated interests before outputting it.
			    // Exercise K2
				console.printf("username:   " + userRecord.getValue("username")
						+ "\n");
				console.printf("password:   " + userRecord.getValue("password")
						+ "\n");
				console.printf("gender:     " + userRecord.getValue("gender") + "\n");
				console.printf("region:     " + userRecord.getValue("region") + "\n");
				console.printf("tweetcount: " + userRecord.getValue("tweetcount") + "\n");
				console.printf("interests:  " + userRecord.getValue("interests") + "\n");
			} else {
				console.printf("ERROR: User record not found!\n");
			}		
		} else {
			console.printf("ERROR: Invalid user name.\n");
		}		
	} //getUser

    public void updatePasswordUsingUDF() throws AerospikeException
    {
    	Record userRecord = null;
        Key userKey = null;

        // Get username
        String username;
        console.printf("\nEnter username:");
        username = console.readLine();

        if (username != null && username.length() > 0)
        {
            // TODO: Read user record and check if userRecord exists
            // Exercise R2
        	
            userKey = new Key("test", "users", username);
            userRecord = client.get(null, userKey);
            if (userRecord != null)
            {
                // Get new password
                String password;
                console.printf("Enter new password for " + username + ":");
                password = console.readLine();

                // TODO: Register UDF
                // Exercise R2
                // NOTE: UDF registration has been included here for demonstration of syntax 
                // The recommended way of registering UDFs in production env is  via AQL
                // or standalone application routines using code similar to below.
                LuaConfig.SourceDirectory = "udf";
    			File udfFile = new File("udf/updateUserPwd.lua");

    			RegisterTask rt = client.register(null, udfFile.getPath(),
					udfFile.getName(), Language.LUA);
    			rt.waitTillComplete(100);

                // TODO: Execute UDF
                // Exercise R2
    			String updatedPassword = client.execute(null, userKey, "updateUserPwd", "updatePassword", Value.get(password)).toString();
                
                // TODO: Output updated password to the console
                // Exercise R2
                console.printf("\nINFO: The password has been set to: " + updatedPassword);                
            }
            else
            {
            	console.printf("ERROR: User record not found!");
            }
        }
        else
        {
        	console.printf("ERROR: Invalid user name.");
        }
    } //updatePasswordUsingUDF
	
    public void updatePasswordUsingCAS() throws AerospikeException
    {
		Record userRecord = null;
		Key userKey = null;
		Bin passwordBin = null;

		// Get username
		String username;
		console.printf("\nEnter username:");
		username = console.readLine();

		if (username != null && username.length() > 0)
		{
			// Check if username exists
			userKey = new Key("test", "users", username);
			userRecord = client.get(null, userKey);
			if (userRecord != null)
			{
				// Get new password
				String password;
				console.printf("Enter new password for " + username + ":");
				password = console.readLine();
				
				// TODO: Update User record with new password only if generation is the same
				// Exercise K5
				WritePolicy writePolicy = new WritePolicy();
				// record generation
				writePolicy.generation = userRecord.generation;
				writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
				// password Bin
				passwordBin = new Bin("password", Value.get(password));
				client.put(writePolicy, userKey, passwordBin);

				console.printf("\nINFO: The password has been set to: " + password);
			}
			else
			{
				console.printf("ERROR: User record not found!");
			}
		}
		else
		{
			console.printf("ERROR: Invalid user name.");
		}
    } //updatePasswordUsingCAS

	public void batchGetUserTweets() throws AerospikeException {

		Record userRecord = null;
		Key userKey = null;

		// Get username
		String username;
		console.printf("\nEnter username:");
		username = console.readLine();

		if (username != null && username.length() > 0) {
			// TODO: Read user record
			// Exercise K3			
			userKey = new Key("test", "users", username);
			userRecord = client.get(null, userKey);
			
			// Check if userRecord exists
			if (userRecord != null) {
				// TODO: Get how many tweets the user has
				// Exercise K3
				int tweetCount = ((Long) userRecord.getValue("tweetcount")).intValue();

				// TODO: Create an array of tweet keys -- keys[tweetCount]
				// Exercise K3
				Key[] keys = new Key[tweetCount];
				for (int i = 0; i < keys.length; i++) {
					keys[i] = new Key("test", "tweets",
							(username + ":" + (i + 1)));
				}

				console.printf("\nHere's " + username + "'s tweet(s):\n");
				
				// TODO: Initiate batch read operation
				// Exercise K3
				if (keys.length > 0){
					Record[] records = client.get(new BatchPolicy(), keys);
					
					// TODO: Output tweets to the console
					// Exercise K3
					for (int j = 0; j < records.length; j++) {
						console.printf(records[j].getValue("tweet").toString() + "\n");
					}
				}
			}
			else
            {
            	console.printf("ERROR: User record not found!");
            }
        }
        else
        {
        	console.printf("ERROR: Invalid user name.");
        }
	} //batchGetUserTweets

	public void aggregateUsersByTweetCountByRegion() throws AerospikeException,
			InterruptedException {
		//TODO: Create NUMERIC index on tweetcount in users set (Same as Exercise Q4)
		//Exercise A2
		// NOTE: Index creation has been included in here for convenience and to demonstrate the syntax. 
		// The recommended way of creating indexes in production env is via AQL
		// or create once using standalone application code.
		// Do Index creation within try-catch. If index already exists, indexCreate() will error out.
		try {
		  IndexTask task = client.createIndex(null, "test", "users",
				"tweetcount_index", "tweetcount", IndexType.NUMERIC);
		  task.waitTillComplete(100);
		} catch (Exception e) {
		   System.out.printf(e.toString());
		}
		
		ResultSet rs = null;
		try {
			int min;
			int max;
			console.printf("\nEnter Min Tweet Count:");
			min = Integer.parseInt(console.readLine());
			console.printf("Enter Max Tweet Count:");
			max = Integer.parseInt(console.readLine());

			// TODO: Register UDF
            // Exercise A2
            // NOTE: UDF registration has been included here for demonstration of syntax 
            // The recommended way of registering UDFs in production env is  via AQL
            // or standalone application routines using code similar to below.
            LuaConfig.SourceDirectory = "udf";
			File udfFile = new File("udf/aggregationByRegion.lua");

			RegisterTask rt = client.register(null, udfFile.getPath(),
					udfFile.getName(), Language.LUA);
			rt.waitTillComplete(100);

			// TODO: Create String array of bins you would like to retrieve. 
			// In this example, we want to output which region has how many tweets. 
			// Exercise A2
			String[] bins = { "tweetcount", "region" };
			
			// TODO: Create Statement instance
			// Exercise A2
			Statement stmt = new Statement();
			
			// TODO: Set namespace on the instance of Statement
		    // Exercise A2			
			stmt.setNamespace("test");
			
			// TODO: Set name of the set on the instance of Statement
		    // Exercise A2			
			stmt.setSetName("users");
			
			// TODO: Set name of the index on the instance of Statement
		    // Exercise A2			
			stmt.setIndexName("tweetcount_index");
			
			// TODO: Set list of bins you want retrieved on the instance of Statement
		    // Exercise A2			
			stmt.setBinNames(bins);
			
			// TODO: Set min--max range Filter on tweetcount on the instance of Statement
		    // Exercise A2
			stmt.setFilter(Filter.range("tweetcount", min, max));

			// TODO: Execute Aggregation query passing in <null> policy and instance of Statement,
			// Lua module and module function to call.
		    // Exercise A2
			rs = client.queryAggregate(null, stmt, "aggregationByRegion", "sum");

			console.printf("\nAggregating users with " + min + "-"
					+ max + " tweets by region. Hang on...\n");

			// TODO: Iterate through returned RecordSet and for each record, 
			// output text in format "Total Users in <region>: <#>"
		    // Exercise A2
			if (rs.next()) {
				@SuppressWarnings("unchecked")
				Map<Object, Object> result = (Map<Object, Object>) rs
						.getObject();
				console.printf("\nTotal Users in North: " + result.get("n") + "\n");
				console.printf("Total Users in South: " + result.get("s") + "\n");
				console.printf("Total Users in East: " + result.get("e") + "\n");
				console.printf("Total Users in West: " + result.get("w") + "\n");
			}
		} finally {
			// TODO: Close record set 
			// Exercise A2
			if (rs != null) {
				// Close result set
				rs.close();
			}
		}
	} //aggregateUsersByTweetCountByRegion

	public void createUsers() throws AerospikeException {
		String[] genders = { "m", "f" };
		String[] regions = { "n", "s", "e", "w" };
        String[] randomInterests = { "Music", "Football", "Soccer", "Baseball", "Basketball", "Hockey", "Weekend Warrior", "Hiking", "Camping", "Travel", "Photography"};
        String username;
        ArrayList<Object> userInterests = null;
        int totalInterests = 0;
		int start = 1;
		int end = 10000;
		int totalUsers = end - start + 1;
		Random rnd1 = new Random();
		Random rnd2 = new Random();
		Random rnd3 = new Random();

		WritePolicy wPolicy = new WritePolicy();
		wPolicy.recordExistsAction = RecordExistsAction.UPDATE;

		console.printf("\nCreate " + totalUsers
				+ " users. Press any key to continue...\n");
		console.readLine();

		for (int j = start; j <= end; j++) {
			// Write user record
			username = "user" + j;
			Key key = new Key("test", "users", username);
			Bin bin1 = new Bin("username", "user" + j);
			Bin bin2 = new Bin("password", "pwd" + j);
			Bin bin3 = new Bin("gender", genders[rnd1.nextInt(2)]);
			Bin bin4 = new Bin("region", regions[rnd2.nextInt(4)]);
			Bin bin5 = new Bin("lasttweeted", 0);
			Bin bin6 = new Bin("tweetcount", 0);
			
			totalInterests = rnd3.nextInt(7);
			userInterests = new ArrayList<Object>();
			for(int i = 0; i < totalInterests; i++) {
				userInterests.add(randomInterests[rnd3.nextInt(randomInterests.length)]);
			}
			Bin bin7 = new Bin("interests", userInterests);
			
			client.put(wPolicy, key, bin1, bin2, bin3, bin4, bin5, bin6, bin7);
			console.printf("Wrote user record for " + username + "\n");
		}
		console.printf("\nDone creating " + totalUsers + " users!\n");
	} // createUsers

}
