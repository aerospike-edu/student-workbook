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
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

/**
 * @author Dash Desai
 */
public class Program {
	
	private static Logger log = Logger.getLogger(Program.class);
	
	private AerospikeClient mClient;
	private WritePolicy mWritePolicy;
	private Policy mPolicy;
	private EclipseConsole mConsole = new EclipseConsole();

	

	public Program() throws AerospikeException {
		
		//Establish a connection to Aerospike cluster
		ClientPolicy clientPolicy = new ClientPolicy();
		clientPolicy.timeout = 500;
		
		//Set credentials
		clientPolicy.user = "superman";
		clientPolicy.password = "krypton";
		
		mClient = new AerospikeClient(clientPolicy, "127.0.0.1", 3000);
		mWritePolicy = new WritePolicy();
		mWritePolicy.timeout = 100;
		mPolicy = new Policy();
		mPolicy.timeout = 100;
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
		
		mConsole.printf("***** Welcome to Aerospike Developer Training *****\n");
		try {
			mConsole.printf("INFO: Connecting to Aerospike cluster...");

			// Establish connection to Aerospike server

			if (mClient == null || !mClient.isConnected()) {
				mConsole.printf("\nERROR: Connection to Aerospike cluster failed! Please check the server settings and try again!");
				mConsole.readLine();
			} else {
				mConsole.printf("\nINFO: Connection to Aerospike cluster succeeded!\n");
				
				// Create instance of UserService
				UserService us = new UserService(mClient);
				// Create instance of RoleService
				RoleService rs = new RoleService(mClient);

				// Present options
				mConsole.printf("\nWhat would you like to do:\n");
				mConsole.printf("1> Create User\n");
				mConsole.printf("2> Read User\n");
				mConsole.printf("3> Grant Role to User\n");
				mConsole.printf("4> Revoke Role from User\n");
				mConsole.printf("5> Drop User\n");
				mConsole.printf("6> Create Role\n");
				mConsole.printf("7> Read Role\n");
				mConsole.printf("8> Grant Privilege to Role\n");
				mConsole.printf("9> Revoke Privilege from Role\n");
				mConsole.printf("10> Drop Role\n");
				mConsole.printf("0> Exit\n");
				mConsole.printf("\nSelect 0-10 and hit enter:\n");
				int feature = Integer.parseInt(mConsole.readLine());
				
				if (feature != 0) {
					switch (feature) {
					case 1:
						mConsole.printf("\n********** Your Selection: Create User**********\n");
						us.createUser();
						break;
					case 2:
						mConsole.printf("\n********** Your Selection: Read User **********\n");
						us.getUser();
						break;
					case 3:
						mConsole.printf("\n********** Your Selection: Grant Role to User **********\n");
						us.grantRole();
						break;
					case 4:
						mConsole.printf("\n********** Your Selection: Revoke Role from User **********\n");
						 us.revokeRole();
						break;
					case 5:
						mConsole.printf("\n********** Your Selection: Drop User **********\n");
						us.dropUser();
						break;
					case 6:
						mConsole.printf("\n********** Your Selection: Create Role **********\n");
						rs.createRole();
						break;
					case 7:
						mConsole.printf("\n********** Your Selection: Read Role **********\n");
						rs.getRole();
						break;
					case 8:
						mConsole.printf("\n********** Grant Privilege to Role **********\n");
						rs.grantPrivilege();
						break;
					case 9:
						mConsole.printf("\n********** Revoke Privilege from Role **********\n");
						rs.revokePrivilege();
						break;
					case 10:
						mConsole.printf("\n********** Drop Role **********\n");
						rs.dropRole();
						break;
					default:
						break;
					}
				}
			}
		} catch (AerospikeException e) {
			mConsole.printf("AerospikeException - Message: " + e.getMessage() + "\n");
			mConsole.printf("AerospikeException - StackTrace: " + Service.printStackTrace(e) + "\n");
		} catch (Exception e) {
			mConsole.printf("Exception - Message: " + e.getMessage() + "\n");
			mConsole.printf("Exception - StackTrace: " + Service.printStackTrace(e) + "\n");
		} finally {
			if (mClient != null) {
				// Close Aerospike server connection
				mClient.close();
			}
			mConsole.printf("\n\nINFO: Press any key to exit...\n");
			mConsole.readLine();
		}
	}
}