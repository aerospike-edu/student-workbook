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

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.admin.User;
import com.aerospike.client.policy.AdminPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class UserService extends Service {
	private AerospikeClient mClient;
	private EclipseConsole console = new EclipseConsole();
	private Gson gson = new GsonBuilder().setPrettyPrinting().create();

	public UserService(AerospikeClient client) {
		this.mClient = client;
	}

	public void createUser() throws AerospikeException {
		console.printf("\n********** Create User **********\n");

		String username;
		String password;
		String role;

		//Get username
		console.printf("Enter username: ");
		username = console.readLine();

		if (username != null && username.length() > 0) {
			// Get password
			console.printf("Enter password for " + username + ":");
			password = console.readLine();

			// Get role, can be null
			console.printf("Enter a role for " + username + ":");
			role = console.readLine();

			// Create user
			AdminPolicy adminPolicy = new AdminPolicy();
			List<String> roles = new ArrayList<String>(1);
			roles.add(role);
			
		    // TODO: Create user
		    // Exercise 1

			console.printf("\nINFO: User record created!");
		}
	} //createUser

	public void getUser() throws AerospikeException {

		// Get username
		String username;
		console.printf("\nEnter username:");
		username = console.readLine();

		if (username != null && username.length() > 0) {
			
			// TODO: Read user
		    // Exercise 2
			User user = null;
			
			if (user != null) {
				console.printf("\nINFO: User read successfully! Here are the details:\n");
				console.printf(gson.toJson(user) + "\n");
			} else {
				console.printf("ERROR: User record not found!\n");
			}		
		} else {
			console.printf("ERROR: User record not found!\n");
		}		
	} //getUser

	public void updatePassword() throws AerospikeException {

		// Get username
		String username;
		console.printf("\nEnter username:");
		username = console.readLine();

		if (username != null && username.length() > 0) {
			// Get new password
			String password;
			console.printf("Enter new password for " + username + ":");
			password = console.readLine();

			// TODO: Update user password
		    // Exercise 3
			
			console.printf("\nINFO: The password has been set to: " + password);
		} else {
			console.printf("ERROR: User record not found!");
		}
	} //updatePassword
	
	
	public void dropUser() throws AerospikeException {
		// Get username
		String username;
		console.printf("\nEnter username:");
		username = console.readLine();

		if (username != null && username.length() > 0) {

			// TODO: Drop user
		    // Exercise 4
			
			console.printf("\nINFO: The user has been dropped.");
		} else {
			console.printf("ERROR: User record not found!");
		}
	}
	
	public void grantRole() throws AerospikeException {

		// Get username
		String username;
		console.printf("\nEnter username:");
		username = console.readLine();

		if (username != null && username.length() > 0) {
			// Get new password
			String role;
			console.printf("Enter new role for " + username + ":");
			role = console.readLine();

			AdminPolicy adminPolicy = new AdminPolicy();
			List<String> roles = new ArrayList<String>(1);
			roles.add(role);

			// TODO: Add Role
		    // Exercise 5
			
			console.printf("\nINFO: The role has been added to: " + username);
		} else {
			console.printf("ERROR: User record not found!");
		}
	}
	
	public void revokeRole() throws AerospikeException {

		// Get username
		String username;
		console.printf("\nEnter username:");
		username = console.readLine();

		if (username != null && username.length() > 0) {
			// Get new password
			String role;
			console.printf("Enter role to remove from " + username + ":");
			role = console.readLine();

			AdminPolicy adminPolicy = new AdminPolicy();
			List<String> roles = new ArrayList<String>(1);
			roles.add(role);

			// TODO: Revoke role
		    // Exercise 6			
			
			console.printf("\nINFO: The role has been removed from: " + username);
		} else {
			console.printf("ERROR: User record not found!");
		}
	}
}