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
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.task.RegisterTask;

public class UserService {
	private AerospikeClient client;
	private EclipseConsole console = new EclipseConsole();

	public UserService(AerospikeClient client) {
		this.client = client;
	}

	public void createUser() throws AerospikeException {
		console.printf("\n********** Create User **********\n");

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
			WritePolicy wPolicy = new WritePolicy();
			wPolicy.recordExistsAction = RecordExistsAction.UPDATE;

			Key key = new Key("test", "users", username);
			Bin bin1 = new Bin("username", username);
			Bin bin2 = new Bin("password", password);
			Bin bin3 = new Bin("gender", gender);
			Bin bin4 = new Bin("region", region);
			Bin bin5 = new Bin("lasttweeted", 0);
			Bin bin6 = new Bin("tweetcount", 0);
			Bin bin7 = new Bin("interests", Arrays.asList(interests.split(",")));

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
			// Check if username exists
			userKey = new Key("test", "users", username);
			userRecord = client.get(null, userKey);
			if (userRecord != null) {
				console.printf("\nINFO: User record read successfully! Here are the details:\n");
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
			console.printf("ERROR: User record not found!\n");
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
        // Check if username exists
        userKey = new Key("test", "users", username);
        userRecord = client.get(null, userKey);
        if (userRecord != null)
        {
            // Get new password
            String password;
            console.printf("Enter new password for " + username + ":");
            password = console.readLine();

            // NOTE: UDF registration has been included here for convenience and to demonstrate the syntax. The recommended way of registering UDFs in production env is via AQL
	    			LuaConfig.SourceDirectory = "udf";
	    			File udfFile = new File("udf/updateUserPwd.lua");

	    			RegisterTask rt = client.register(null, udfFile.getPath(),
  					udfFile.getName(), Language.LUA);
	    			rt.waitTillComplete(100);

            String updatedPassword = client.execute(null, userKey, "updateUserPwd", "updatePassword", Value.get(password)).toString();
            console.printf("\nINFO: The password has been set to: " + updatedPassword);
        }
        else
        {
        	console.printf("ERROR: User record not found!");
        }
    }
    else
    {
    	console.printf("ERROR: User record not found!");
    }
  } //updatePasswordUsingUDF
	
	public void batchGetUserTweets() throws AerospikeException {

		Record userRecord = null;
		Key userKey = null;

		// Get username
		String username;
		console.printf("\nEnter username:");
		username = console.readLine();

		if (username != null && username.length() > 0) {
			// Check if username exists
			userKey = new Key("test", "users", username);
			userRecord = client.get(null, userKey);
			if (userRecord != null) {
				// Get how many tweets the user has
				int tweetCount = (Integer) userRecord.getValue("tweetcount");

				// Create an array of keys so we can initiate batch read
				// operation
				Key[] keys = new Key[tweetCount];
				for (int i = 0; i < keys.length; i++) {
					keys[i] = new Key("test", "tweets",
							(username + ":" + (i + 1)));
				}

				console.printf("\nHere's " + username + "'s tweet(s):\n");
				
				// Initiate batch read operation
				if (keys.length > 0){
					Record[] records = client.get(new BatchPolicy(), keys);
					for (int j = 0; j < records.length; j++) {
						console.printf(records[j].getValue("tweet").toString() + "\n");
					}
				}
			}
		} else {
			console.printf("ERROR: User record not found!\n");
		}
	} //batchGetUserTweets

	public void aggregateUsersByTweetCountByRegion() throws AerospikeException,
			InterruptedException {
	} //aggregateUsersByTweetCountByRegion

	public void createUsers() throws AerospikeException {
		String[] genders = { "m", "f" };
		String[] regions = { "n", "s", "e", "w" };
        String[] randomInterests = { "Music", "Football", "Soccer", "Baseball", "Basketball", "Hockey", "Weekend Warrior", "Hiking", "Camping", "Travel", "Photography"};
        String username;
        ArrayList<Object> userInterests = null;
        int totalInterests = 0;
		int start = 1;
		int end = 100000;
		int totalUsers = end - start;
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
		console.printf("\nDone creating " + totalUsers + "!\n");
	} // createUsers

}