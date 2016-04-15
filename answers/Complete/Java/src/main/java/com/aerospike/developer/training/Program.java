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

import org.apache.log4j.Logger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

/**
 * @author Dash Desai
 */
public class Program {
	private AerospikeClient client;
	private WritePolicy writePolicy;
	private Policy policy;
	private EclipseConsole console = new EclipseConsole();

	private static Logger log = Logger.getLogger(Program.class);

	public Program()
			throws AerospikeException {
		// Establish a connection to Aerospike cluster
		ClientPolicy cPolicy = new ClientPolicy();
		cPolicy.timeout = 500;
		this.client = new AerospikeClient(cPolicy, "127.0.0.1", 3000);
		this.writePolicy = new WritePolicy();
		this.writePolicy.timeout = 100;
		this.policy = new Policy();
		this.policy.timeout = 100;
	}

	public static void main(String[] args) throws AerospikeException {
		try {

			Program as = new Program();

			as.work();

		} catch (Exception e) {
			log.error("Critical error", e);
		}
	}


	public void work() throws Exception {
		console.printf("***** Welcome to Aerospike Developer Training *****\n");
		try {
			console.printf("INFO: Connecting to Aerospike cluster...");

			// Establish connection to Aerospike server

			if (client == null || !client.isConnected()) {
				console.printf("\nERROR: Connection to Aerospike cluster failed! Please check the server settings and try again!");
				console.readLine();
			} else {
				console.printf("\nINFO: Connection to Aerospike cluster succeeded!\n");

				// Create instance of UserService
				UserService us = new UserService(client);
				// Create instance of TweetService
				TweetService ts = new TweetService(client);
				// Create instance of UtilityService
				UtilityService util = new UtilityService(client);

				// Present options
				console.printf("\nWhat would you like to do:\n");
				console.printf("1> Create A User And A Tweet\n");
				console.printf("2> Read A User Record\n");
				console.printf("3> Batch Read Tweets For A User\n");
				console.printf("4> Scan All Tweets For All Users\n");
				console.printf("5> Record UDF -- Update User Password\n");
				console.printf("6> Query Tweets By Username And Users By Tweet Count Range\n");
				console.printf("7> Stream UDF -- Aggregation Based on Tweet Count By Region\n");
				console.printf("0> Exit\n");
				console.printf("\nSelect 0-7 and hit enter:\n");
				int feature = Integer.parseInt(console.readLine());
				
				if (feature != 0) {
					switch (feature) {
					case 1:
						console.printf("\n********** Your Selection: Create User And A Tweet **********\n");
						us.createUser();
						ts.createTweet();
						break;
					case 2:
						console.printf("\n********** Your Selection: Read A User Record **********\n");
						us.getUser();
						break;
					case 3:
						console.printf("\n********** Your Selection: Batch Read Tweets For A User **********\n");
						us.batchGetUserTweets();
						break;
					case 4:
						console.printf("\n********** Your Selection: Scan All Tweets For All Users **********\n");
						 ts.scanAllTweetsForAllUsers();
						break;
					case 5:
						console.printf("\n********** Your Selection: Record UDF -- Update User Password **********\n");
						us.updatePasswordUsingUDF();
						break;
					case 6:
						console.printf("\n********** Your Selection: Query Tweets By Username And Users By Tweet Count Range **********\n");
						ts.queryTweets();
						break;
					case 7:
						console.printf("\n********** Your Selection: Stream UDF -- Aggregation Based on Tweet Count By Region **********\n");
						us.aggregateUsersByTweetCountByRegion();
						break;
					case 12:
						console.printf("\n********** Create Users **********\n");
						us.createUsers();
						break;
					case 23:
						console.printf("\n********** Create Tweets **********\n");
						ts.createTweets();
						break;
					case 33:
						console.printf("\n********** Cluster Information **********\n");
						us.clusterInformation();
						break;
					default:
						break;
					}
				}
			}
		} catch (AerospikeException e) {
			console.printf("AerospikeException - Message: " + e.getMessage()
					+ "\n");
			console.printf("AerospikeException - StackTrace: "
					+ UtilityService.printStackTrace(e) + "\n");
		} catch (Exception e) {
			console.printf("Exception - Message: " + e.getMessage() + "\n");
			console.printf("Exception - StackTrace: "
					+ UtilityService.printStackTrace(e) + "\n");
		} finally {
			if (client != null && client.isConnected()) {
				// Close Aerospike server connection
				client.close();
			}
			console.printf("\n\nINFO: Press any key to exit...\n");
			console.readLine();
		}
	}

	/*
	 * example method calls
	 */
	public Record readPartial(String userName) throws AerospikeException {
		// Java read specific bins
		Key key = new Key("test", "users", userName);
		Record record = this.client.get(null, key, "username", "password",
				"gender", "region");
		return record;
	}

	public Record readMeta(String userName) throws AerospikeException {
		// Java get meta data
		Key key = new Key("test", "users", userName);
		Record record = this.client.getHeader(null, key);
		return record;
	}

	public void write(String username, String password)
			throws AerospikeException {
		// Java read-modify-write
		WritePolicy wPolicy = new WritePolicy();
		wPolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;

		Key key = new Key("test", "users", username);
		Bin bin1 = new Bin("username", username);
		Bin bin2 = new Bin("password", password);

		client.put(wPolicy, key, bin1, bin2);
	}

	public void delete(String username) throws AerospikeException {
		// Java Delete record.
		WritePolicy wPolicy = new WritePolicy();
		Key key = new Key("test", "users", username);
		client.delete(wPolicy, key);
	}

	public boolean exisis(String username) throws AerospikeException {
		// Java exists
		Key key = new Key("test", "users", username);
		boolean itsHere = client.exists(policy, key);
		return itsHere;
	}

	public void add(String username) throws AerospikeException {
		// Java add
		WritePolicy wPolicy = new WritePolicy();
		Key key = new Key("test", "users", username);
		Bin counter = new Bin("tweetcount", 1);
		client.add(wPolicy, key, counter);
	}

	public void touch(String username) throws AerospikeException {
		// Java touch
		WritePolicy wPolicy = new WritePolicy();
		Key key = new Key("test", "users", username);
		client.touch(wPolicy, key);
	}

	public void append(String username) throws AerospikeException {
		// Java append
		WritePolicy wPolicy = new WritePolicy();
		Key key = new Key("test", "users", username);
		Bin bin2 = new Bin("interests", "cats");
		client.append(wPolicy, key, bin2);
	}

	public void operate(String username) throws AerospikeException {
		// Java operate
		WritePolicy wPolicy = new WritePolicy();
		Key key = new Key("test", "users", username);
		client.operate(wPolicy, key, Operation.put(new Bin("tweetcount", 153)),
				Operation.put(new Bin("lasttweeted", 1406755079L)));

	}

	@SuppressWarnings("unused")
	public void batch(String username) throws AerospikeException {
		// Java batch
		// Create an array of keys so we can initiate batch read operation
		Key[] keys = new Key[27];
		for (int i = 0; i < keys.length; i++) {
			keys[i] = new Key("test", "tweets", (username + ":" + (i + 1)));
		}

		// Initiate batch read operation
		Record[] records = client.get(null, keys);

	}

	@SuppressWarnings({ "unused", "resource" })
	public void multipleSeedNodes() throws AerospikeException {
		Host[] hosts = new Host[] { new Host("a.host", 3000),
				new Host("another.host", 3000),
				new Host("and.another.host", 3000) };
		AerospikeClient client = new AerospikeClient(new ClientPolicy(), hosts);

	}
	@SuppressWarnings({ "unused", "resource" })
	public void connectWithClientPolicy() throws AerospikeException {
		// Java connection with Client policy
		ClientPolicy clientPolicy = new ClientPolicy();
		clientPolicy.maxSocketIdle = 3; // 3 seconds
		AerospikeClient client = new AerospikeClient(clientPolicy, "a.host", 3000);

	}
	
	public void deleteBin(String username) throws AerospikeException{
		// Java delete a bin
		WritePolicy wPolicy = new WritePolicy();
		Key key = new Key("test", "users", username);
		Bin bin1 = Bin.asNull("shoe-size"); // Set bin value to null to drop bin.
		client.put(wPolicy, key, bin1);
	}

}