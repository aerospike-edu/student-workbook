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
import java.util.Random;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.Value;
import com.aerospike.client.lua.LuaConfig;
import com.aerospike.client.policy.Priority;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;

public class TweetService {
	private AerospikeClient client;
	private EclipseConsole console = new EclipseConsole();

	public TweetService(AerospikeClient client) {
		this.client = client;
	}

	public void createTweet() throws AerospikeException, InterruptedException {

		console.printf("\n********** Create Tweet **********\n");

	    ///*********************///
	    ///*****Data Model*****///
	    //Namespace: test
	    //Set: tweets
	        //Key: <username:<counter>>
	        //Bins:
	            //tweet - string
	            //ts - int (Stores epoch timestamp of the tweet)
	            //username - string

		    //Sample Key: dash:1
		    //Sample Record:
		        //{ tweet: 'Put. A. Bird. On. It.',
		        //  ts: 1408574221,
		        //  username: 'dash'
		        //}
	    ///*********************///

		Record userRecord = null;
		Key userKey = null;
		Key tweetKey = null;

		// Get username
		String username;
		console.printf("\nEnter username:");
		username = console.readLine();

		if (username != null && username.length() > 0) {
			// TODO: Check if userRecord exists
			// Exercise K2
			userKey = new Key("test", "users", username);
			userRecord = client.get(null, userKey);
			if (userRecord != null) {
				int nextTweetCount = Integer.parseInt(userRecord.getValue(
						"tweetcount").toString()) + 1;

				// Get tweet
				String tweet;
				console.printf("Enter tweet for " + username + ":");
				tweet = console.readLine();

				// Create timestamp to store along with the tweet so we can
				// query, index and report on it
				long ts = getTimeStamp();

				// TODO: Create WritePolicy instance
			    // Exercise K2
				WritePolicy wPolicy = new WritePolicy();
				wPolicy.recordExistsAction = RecordExistsAction.UPDATE;

				// TODO: Create Key and Bin instances for the tweet record.
				// HINT: tweet key should be in username:nextTweetCount format
			    // Exercise K2
				tweetKey = new Key("test", "tweets", username + ":"
						+ nextTweetCount);
				Bin bin1 = new Bin("tweet", tweet);
				Bin bin2 = new Bin("ts", ts);
				Bin bin3 = new Bin("username", username);

				// TODO: Write tweet record
			    // Exercise K2

				client.put(wPolicy, tweetKey, bin1, bin2, bin3);
				console.printf("\nINFO: Tweet record created!\n");

				// TODO: Update tweet count and last tweeted timestamp in the user
				// record. Add code in updateUser()
	            // Exercise K2
				updateUser(client, userKey, wPolicy, ts, nextTweetCount);
			} else {
				console.printf("ERROR: User record not found!\n");
			}
		}
	} //createTweet

	public void scanAllTweetsForAllUsers() {
		try {
			// Java Scan
			// TODO: Create ScanPolicy instance
			// Exercise K4
			ScanPolicy policy = new ScanPolicy();
			policy.concurrentNodes = true;
			policy.priority = Priority.LOW;
			policy.includeBinData = true;

			// TODO: Initiate scan operation that invokes callback for outputting tweets on the console
			// Exercise K4
			client.scanAll(policy, "test", "tweets", new ScanCallback() {

				public void scanCallback(Key key, Record record)
						throws AerospikeException {
					console.printf(record.getValue("tweet") + "\n");

				}
			}, "tweet");
		} catch (AerospikeException e) {
			System.out.println("EXCEPTION - Message: " + e.getMessage());
			System.out.println("EXCEPTION - StackTrace: "
					+ UtilityService.printStackTrace(e));
		}
	} //scanAllTweetsForAllUsers

	private void updateUser(AerospikeClient client, Key userKey,
			WritePolicy policy, long ts, int tweetCount) throws AerospikeException, InterruptedException {

		// TODO: Update tweet count and last tweeted timestamp in the user record
	    // Exercise K2
        client.put(policy, userKey, new Bin("tweetcount", tweetCount), new Bin("lasttweeted", ts));
        console.printf("\nINFO: The tweet count now is: " + tweetCount);


        //Exercise K6
        //Comment above code and uncomment call to updateUserUsingOperate()
        //Add code in updateUserUsingOperate()
        // updateUserUsingOperate(client, userKey, policy, ts);
	} //updateUser

	private void updateUserUsingOperate(AerospikeClient client, Key userKey,
			WritePolicy policy, long ts) throws AerospikeException {

		// TODO: Initiate operate passing in policy, user record key,
		// .add operation incrementing tweet count, .put operation updating timestamp
		// and .get operation to read the user record
	    // Exercise K6

		Record record = client.operate(policy, userKey,
				Operation.add(new Bin("tweetcount", 1)),
				Operation.put(new Bin("lasttweeted", ts)),
				Operation.get());

		// TODO: Output most recent tweet count
	    // Exercise K6
		console.printf("\nINFO: The tweet count now is: " + record.getValue("tweetcount"));
	} //updateUserUsingOperate

	public void queryTweetsByUsername() throws AerospikeException {
		console.printf("\n********** Query Tweets By Username **********\n");

		//TODO: Create STRING index on username in tweets set
		//Exercise Q3
		// NOTE: Index creation has been included in here for convenience and to demonstrate the syntax.
	    // The recommended way of creating indexes in production env is via AQL
		// or create once using standalone application code.

		console.printf("TODO: Create STRING index on username in tweets set.\n");
    // Do Index creation within try-catch. If index already exists, indexCreate() will error out.
		//try {
    //  IndexTask task = .....
		//  task.waitTillComplete(100);
    // } catch (Exception e) {
    // System.out.printf(e.toString());
    // }

		RecordSet rs = null;
		try {
			// Get username
			String username;
			console.printf("\nEnter username:");
			username = console.readLine();

			if (username != null && username.length() > 0) {
				// TODO: Create String array of bins you would like to retrieve.
				// In this example, we want to display all tweets for a given user.
				// Exercise Q3
				console.printf("TODO: Create String array of bins you would like to retrieve.\n");
				//String[] bins = .....

				// TODO: Create Statement instance
				// Exercise Q3
				console.printf("TODO: Create Statement instance.\n");

				// TODO: Set namespace on the instance of Statement
				// Exercise Q3
				console.printf("TODO: Set namespace on the instance of Statement.\n");

				// TODO: Set name of the set on the instance of Statement
				// Exercise Q3
				console.printf("TODO: Set name of the set on the instance of Statement.\n");

				// TODO: Set name of the index on the instance of Statement
			    // Exercise Q3
				console.printf("TODO: Set name of the index on the instance of Statement.\n");


				// TODO: Set list of bins you want retrieved on the instance of Statement
				// Exercise Q3
				console.printf("TODO: Set list of bins you want retrieved on the instance of Statement.\n");

				// TODO: Set equality Filter on username on the instance of Statement
			    // Exercise Q3
				console.printf("TODO: Set equality Filter on username on the instance of Statement.\n");

				// TODO: Execute query passing in <null> policy and instance of Statement
				// Exercise Q3
				console.printf("TODO: Execute query passing in <null> policy and instance of Statement.\n");


				console.printf("\nHere's " + username + "'s tweet(s):\n");
				console.printf("TODO: Iterate through returned RecordSet and output tweets to the console.\n");
				while (rs.next()) {
					Record r = rs.getRecord();
					// TODO: Iterate through returned RecordSet and output tweets to the console
					// Exercise Q3
					// console.printf(.....
				}
			} else {
				console.printf("ERROR: User record not found!\n");
			}
		} finally {
			// TODO: Close record set
			// Exercise Q3
			console.printf("TODO: Close record set.\n");
		}
	} //queryTweetsByUsername

	public void queryUsersByTweetCount() throws AerospikeException {
		console.printf("\n********** Query Users By Tweet Count Range **********\n");

		//TODO: Create NUMERIC index on tweetcount in users set
		//Exercise Q4
		// NOTE: Index creation has been included in here for convenience and to demonstrate the syntax.
		// The recommended way of creating indexes in production env is via AQL
		// or create once using standalone application code.

		console.printf("TODO: Create NUMERIC index on tweetcount in users set.\n");

    // Do Index creation within try-catch. If index already exists, indexCreate() will error out.
		//try {
    //  IndexTask task = .....
		//  task.waitTillComplete(100);
    // } catch (Exception e) {
    // System.out.printf(e.toString());
    // }

		RecordSet rs = null;
		try {
			// Get min and max tweet counts
			int min;
			int max;
			console.printf("\nEnter Min Tweet Count:");
			min = Integer.parseInt(console.readLine());
			console.printf("Enter Max Tweet Count:");
			max = Integer.parseInt(console.readLine());

            // TODO: Create String array of bins you would like to retrieve.
			// In this example, we want to output which user has how many tweets.
            // Exercise Q4
			console.printf("TODO: Create String array of bins you would like to retrieve..\n");
			//String[] bins = .....

			// TODO: Create Statement instance
			// Exercise Q4
			console.printf("TODO: Create Statement instance.\n");


			// TODO: Set namespace on the instance of Statement
		    // Exercise Q4
			console.printf("TODO: Set namespace on the instance of Statement.\n");


			// TODO: Set name of the set on the instance of Statement
		    // Exercise Q4
			console.printf("TODO: Set name of the set on the instance of Statement.\n");


			// TODO: Set name of the index on the instance of Statement
		    // Exercise Q4
			console.printf("TODO: Set name of the index on the instance of Statement.\n");


			// TODO: Set list of bins you want retrieved on the instance of Statement
		    // Exercise Q4
			console.printf("TODO: Set list of bins you want retrieved on the instance of Statement.\n");


			// TODO: Set min--max range Filter on tweetcount on the instance of Statement
		    // Exercise Q4
			console.printf("TODO: Set min--max range Filter on tweetcount on the instance of Statement.\n");


			// TODO: Execute query passing in <null> policy and instance of Statement
		    // Exercise Q4
			console.printf("TODO: Execute query passing in <null> policy and instance of Statement.\n");


			console.printf("\nList of users with " + min + "-" + max
					+ " tweets:\n");

			console.printf("TODO: Iterate through returned RecordSet and for each record, output text in format <username> has <#> tweets.\n");
			while (rs.next()) {
				Record r = rs.getRecord();
				// TODO: Iterate through returned RecordSet and for each record, output text in format "<username> has <#> tweets"
			    // Exercise Q4
				//console.printf(....
				}
			} finally {
				// TODO: Close record set
				// Exercise Q4
				console.printf("TODO: Close record set.\n");
		}
	} //queryUsersByTweetCount

	public void createTweets() throws AerospikeException {
		String[] randomTweets = {
				"For just $1 you get a half price download of half of the song and listen to it just once.",
				"People tell me my body looks like a melted candle",
				"Come on movie! Make it start!", "Byaaaayy",
				"Please, please, win! Meow, meow, meow!",
				"Put. A. Bird. On. It.",
				"A weekend wasted is a weekend well spent",
				"Would you like to super spike your meal?",
				"We have a mean no-no-bring-bag up here on aisle two.",
				"SEEK: See, Every, EVERY, Kind... of spot",
				"We can order that for you. It will take a year to get there.",
				"If you are pregnant, have a soda.",
				"Hear that snap? Hear that clap?",
				"Follow me and I may follow you",
				"Which is the best cafe in Portland? Discuss...",
				"Portland Coffee is for closers!",
				"Lets get this party started!",
				"How about them portland blazers!", "You got school'd, yo",
				"I love animals", "I love my dog", "What's up Portland",
				"Which is the best cafe in Portland? Discuss...",
				"I dont always tweet, but when I do it is on Tweetaspike" };
		Random rnd1 = new Random();
		Random rnd2 = new Random();
		Random rnd3 = new Random();
		Key userKey;
		Record userRecord;
		int totalUsers = 1000;
		int maxUsers = 10000;
		int maxTweets = 20;
		String username;
		long ts = 0;

		WritePolicy wPolicy = new WritePolicy();
		wPolicy.recordExistsAction = RecordExistsAction.UPDATE;

		console.printf("\nCreate up to " + maxTweets + " tweets each for "
				+ totalUsers + " users. Press any key to continue...\n");
		console.readLine();

		for (int j = 0; j < totalUsers; j++) {
			// Check if user record exists
			username = "user" + rnd3.nextInt(maxUsers);
			userKey = new Key("test", "users", username);
			userRecord = client.get(null, userKey);
			if (userRecord != null) {
				// create up to maxTweets random tweets for this user
				int totalTweets = rnd1.nextInt(maxTweets);
				for (int k = 1; k <= totalTweets; k++) {
					// Create timestamp to store along with the tweet so we can
					// query, index and report on it
					ts = getTimeStamp();
					Key tweetKey = new Key("test", "tweets", username + ":" + k);
					Bin bin1 = new Bin("tweet",
							randomTweets[rnd2.nextInt(randomTweets.length)]);
					Bin bin2 = new Bin("ts", ts);
					Bin bin3 = new Bin("username", username);

					client.put(wPolicy, tweetKey, bin1, bin2, bin3);
				}
				console.printf("\nWrote " + totalTweets + " tweets for "
						+ username);
				if (totalTweets > 0) {
					// Update tweet count and last tweet'd timestamp in the user
					// record
			        client.put(wPolicy, userKey, new Bin("tweetcount", totalTweets), new Bin("lasttweeted", ts));
			        //console.printf("\nINFO: The tweet count now is: " + totalTweets);
				}
			}
		}
		console.printf("\n\nDone creating up to " + maxTweets
				+ " tweets each for " + totalUsers + " users!\n");
	} //createTweets

	private long getTimeStamp() {
		return System.currentTimeMillis();
	} //getTimeStamp

}
