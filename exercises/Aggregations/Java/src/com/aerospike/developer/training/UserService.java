/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/

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
			Bin bin7 = Bin.asList("interests", Arrays.asList(interests.split(",")));

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
					Record[] records = client.get(new Policy(), keys);
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
		ResultSet rs = null;
		try {
			int min;
			int max;
			console.printf("\nEnter Min Tweet Count:");
			min = Integer.parseInt(console.readLine());
			console.printf("Enter Max Tweet Count:");
			max = Integer.parseInt(console.readLine());

      // TODO: Register UDF
      // Exercise 2
      // NOTE: UDF registration has been included here for convenience 
      // NOTE: The recommended way of registering UDFs in production env is via AQL
      console.printf("\nTODO: Register UDF");

      // TODO: Create string array of bins to retrieve. In this example, we want to display total users that have tweets between min-max range by region. 
      // Exercise 2
      console.printf("\nTODO: Create string array of bins to retrieve. In this example, we want to display total users that have tweets between min-max range by region.");

      // TODO: Create Statement instance
      // Exercise 2
      console.printf("\nTODO: Create Statement instance");

      // TODO: Set namespace on the instance of Statement
      // Exercise 2
      console.printf("\nTODO: Set namespace on the instance of Statement");

      // TODO: Set name of the set on the instance of Statement
      // Exercise 2
      console.printf("\nTODO: Set name of the set on the instance of Statement");

      // TODO: Set name of the index on the instance of Statement
      // Exercise 2
      console.printf("\nTODO: Set index name on the instance of Statement");

      // TODO: Set list of bins you want retrieved on the instance of Statement
      // Exercise 2
      console.printf("\nTODO: Set list of bins you want retrieved on the instance of Statement");

      // TODO: Set min--max range Filter on tweetcount on the instance of Statement
      // Exercise 2
      console.printf("\nTODO: Set min--max range Filter on tweetcount on the instance of Statement");

      // TODO: Execute aggregate query passing in <null> policy and instance of Statement, .lua filename of the UDF and lua function name.
      // Exercise 2
      console.printf("\nTODO: Execute aggregate query passing in <null> policy and instance of Statement, .lua filename of the UDF and lua function name.");

      // TODO: Examine returned ResultSet and output result to the console in format "Total Users in <region>: <#>"
      // Exercise 2
      console.printf("\nTODO: Examine returned ResultSet and output result to the console in format \"Total Users in <region>: <#>\"");
		} finally {
      // TODO: Close result set 
      // Exercise 2
      console.printf("\nTODO: Close result set");
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
			Bin bin7 = Bin.asList("interests", userInterests);
			
			client.put(wPolicy, key, bin1, bin2, bin3, bin4, bin5, bin6, bin7);
			console.printf("Wrote user record for " + username + "\n");
		}
		console.printf("\nDone creating " + totalUsers + "!\n");
	} // createUsers

}