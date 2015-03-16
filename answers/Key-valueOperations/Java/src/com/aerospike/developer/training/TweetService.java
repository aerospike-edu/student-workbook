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
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
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
			// Check if username exists
			userKey = new Key("test", "users", username);
			userRecord = client.get(null, userKey);
			if (userRecord != null) {
				int nextTweetCount = Integer.parseInt(userRecord.getValue(
						"tweetcount").toString()) + 1;

				// Get tweet
				String tweet;
				console.printf("Enter tweet for " + username + ":");
				tweet = console.readLine();

				// Write record
				WritePolicy wPolicy = new WritePolicy();
				wPolicy.recordExistsAction = RecordExistsAction.UPDATE;

				// Create timestamp to store along with the tweet so we can
				// query, index and report on it
				long ts = getTimeStamp();

				tweetKey = new Key("test", "tweets", username + ":"
						+ nextTweetCount);
				Bin bin1 = new Bin("tweet", tweet);
				Bin bin2 = new Bin("ts", ts);
				Bin bin3 = new Bin("username", username);

				client.put(wPolicy, tweetKey, bin1, bin2, bin3);
				console.printf("\nINFO: Tweet record created!\n");

				// Update tweet count and last tweet'd timestamp in the user
				// record
				updateUser(client, userKey, wPolicy, ts, nextTweetCount);
			} else {
				console.printf("ERROR: User record not found!\n");
			}
		}
	} //createTweet
		
	public void scanAllTweetsForAllUsers() {
		try {
			// Java Scan
			ScanPolicy policy = new ScanPolicy();
			policy.concurrentNodes = true;
			policy.priority = Priority.LOW;
			policy.includeBinData = true;

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
        client.put(policy, userKey, new Bin("tweetcount", tweetCount), new Bin("lasttweeted", ts));
        console.printf("\nINFO: The tweet count now is: " + tweetCount);

        // updateUserUsingOperate(client, userKey, policy, ts);
	} //updateUser

	private void updateUserUsingOperate(AerospikeClient client, Key userKey,
			WritePolicy policy, long ts) throws AerospikeException {
		
		Record record = client.operate(policy, userKey,
				Operation.add(new Bin("tweetcount", 1)),
				Operation.put(new Bin("lasttweeted", ts)),
				Operation.get());
		
		console.printf("\nINFO: The tweet count now is: " + record.getValue("tweetcount"));
	} //updateUserUsingOperate

	public void queryTweetsByUsername() throws AerospikeException {		
		console.printf("\n********** Query Tweets By Username **********\n");
	} //queryTweetsByUsername

	public void queryUsersByTweetCount() throws AerospikeException {
		console.printf("\n********** Query Users By Tweet Count Range **********\n");
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
		int totalUsers = 10000;
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
			username = "user" + rnd3.nextInt(100000);
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
						+ username + "!");
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
