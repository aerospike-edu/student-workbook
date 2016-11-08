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
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
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

      // TODO: Create WritePolicy instance
      // Exercise 2
      console.printf("\nTODO: Create WritePolicy instance");

      // TODO: Create Key and Bin instances for the user record. Remember to convert comma-separated interests into a list before storing it.
      // Exercise 2
      console.printf("\nTODO: Create Key and Bin instances for the user record. Remember to convert comma-separated interests into a list before storing it.");

      // TODO: Write user record
      // Exercise 2
      console.printf("\nTODO: Write user record");

			console.printf("\nINFO: User record NOT created!");
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
      // Exercise 2
      console.printf("\nTODO: Read user record");
			if (userRecord != null) {
				console.printf("\nINFO: User record read successfully! Here are the details:\n");
        // TODO: Output user record to the console. Remember to convert comma-separated interests into a list before outputting it.
	      // Exercise 2
        console.printf("\nTODO: Output user record to the console. Remember to convert comma-separated interests into a list before outputting it");
			} else {
				console.printf("ERROR: User record not found!\n");
			}		
		} else {
			console.printf("ERROR: User record not found!\n");
		}		
	} //getUser

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
				// Exercise 5
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
	} //updatePasswordUsingCAS

  public void updatePasswordUsingUDF() throws AerospikeException
  {
  } //updatePasswordUsingUDF
	
	public void batchGetUserTweets() throws AerospikeException {

		Record userRecord = null;
		Key userKey = null;

		// Get username
		String username;
		console.printf("\nEnter username:");
		username = console.readLine();

		if (username != null && username.length() > 0) {
			// TODO: Read user record
			// Exercise 3
			console.printf("\nTODO: Read user record");

			if (userRecord != null) {
				// TODO: Get how many tweets the user has
				// Exercise 3
				console.printf("\nTODO: Get how many tweets the user has");

	      // TODO: Create an array of tweet keys -- keys[tweetCount]
				// Exercise 3
				console.printf("\nTODO: Create an array of Key instances -- keys[tweetCount]");

	      // TODO: Initiate batch read operation
				// Exercise 3
				console.printf("\nTODO: Initiate batch read operation");

				// TODO: Output tweets to the console
				// Exercise 3
				console.printf("\nTODO: Output tweets to the console");	
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