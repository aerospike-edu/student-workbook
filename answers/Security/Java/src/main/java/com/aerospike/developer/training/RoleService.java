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
import com.aerospike.client.admin.Privilege;
import com.aerospike.client.admin.PrivilegeCode;
import com.aerospike.client.admin.Role;
import com.aerospike.client.policy.AdminPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class RoleService extends Service {
	private AerospikeClient mClient;
	private EclipseConsole console = new EclipseConsole();
	private Gson gson = new GsonBuilder().setPrettyPrinting().create();

	public RoleService(AerospikeClient client) {
		this.mClient = client;
	}

	public void createRole() throws AerospikeException {
		console.printf("\n********** Create Role **********\n");

		String privilegeCode;
		String role;

		//Get role
		console.printf("Enter role: ");
		role = console.readLine();

		if (role != null && role.length() > 0) {

			// Add privilege
			console.printf("Enter a privilege code\n(0 = read, 1 = read-write, 2 = read-write-udf, 3 = data-admin, 4 = sys-admin, 5 = user-admin):");
			privilegeCode = console.readLine();
			Privilege privilege = new Privilege();
			switch (Integer.parseInt(privilegeCode)) {
				case 0 : privilege.code = PrivilegeCode.READ; break;
				case 1 : privilege.code = PrivilegeCode.READ_WRITE; break;
				case 2 : privilege.code = PrivilegeCode.READ_WRITE_UDF; break;
				case 3 : privilege.code = PrivilegeCode.DATA_ADMIN; break;
				case 4 : privilege.code = PrivilegeCode.SYS_ADMIN; break;
				case 5 : privilege.code = PrivilegeCode.USER_ADMIN; break;
			}
			
			// Create user
			AdminPolicy adminPolicy = new AdminPolicy();
			List<Privilege> privileges = new ArrayList<Privilege>(1);
			privileges.add(privilege);
			mClient.createRole(adminPolicy, role, privileges);
			
			console.printf("\nINFO: User record created!");
		}
	}

	public void getRole() throws AerospikeException {

		// Get role
		console.printf("\nEnter role:");
		String roleName = console.readLine();

		if (roleName != null && roleName.length() > 0) {
			
			AdminPolicy adminPolicy = new AdminPolicy();
			Role role = mClient.queryRole(adminPolicy, roleName);
			
			if (role != null) {
				console.printf("\nINFO: Role read successfully! Here are the details:\n");
				console.printf(gson.toJson(role) + "\n");
			} else {
				console.printf("ERROR: Role not found!\n");
			}		
		} else {
			console.printf("ERROR: Role not found!\n");
		}		
	}
	
	public void dropRole() throws AerospikeException {
		// Get role
		console.printf("\nEnter role:");
		String roleName = console.readLine();

		if (roleName != null && roleName.length() > 0) {
			AdminPolicy adminPolicy = new AdminPolicy();
			mClient.dropRole(adminPolicy, roleName);
			console.printf("\nINFO: The role has been dropped.");
		} else {
			console.printf("ERROR: Role not found!");
		}
	}
	
	public void grantPrivilege() throws AerospikeException {

		// Get role
		console.printf("\nEnter role:");
		String roleName = console.readLine();

		if (roleName != null && roleName.length() > 0) {
			// Get new privilege
			console.printf("Enter a privilege code\n(0 = read, 1 = read-write, 2 = read-write-udf, 3 = data-admin, 4 = sys-admin, 5 = user-admin):");
			String privilegeCode = console.readLine();
			Privilege privilege = new Privilege();
			switch (Integer.parseInt(privilegeCode)) {
				case 0 : privilege.code = PrivilegeCode.READ; break;
				case 1 : privilege.code = PrivilegeCode.READ_WRITE; break;
				case 2 : privilege.code = PrivilegeCode.READ_WRITE_UDF; break;
				case 3 : privilege.code = PrivilegeCode.DATA_ADMIN; break;
				case 4 : privilege.code = PrivilegeCode.SYS_ADMIN; break;
				case 5 : privilege.code = PrivilegeCode.USER_ADMIN; break;
			}
			
			// Create user
			AdminPolicy adminPolicy = new AdminPolicy();
			List<Privilege> privileges = new ArrayList<Privilege>(1);
			privileges.add(privilege);
			
			mClient.grantPrivileges(adminPolicy, roleName, privileges);
			console.printf("\nINFO: The privilege has been added to: " + roleName);
		} else {
			console.printf("ERROR: Role not found!");
		}
	}
	
	public void revokePrivilege() throws AerospikeException {

		// Get role
		console.printf("\nEnter role:");
		String roleName = console.readLine();

		if (roleName != null && roleName.length() > 0) {
			// Get new privilege
			console.printf("Enter a privilege code\n(0 = read, 1 = read-write, 2 = read-write-udf, 3 = data-admin, 4 = sys-admin, 5 = user-admin):");
			String privilegeCode = console.readLine();
			Privilege privilege = new Privilege();
			switch (Integer.parseInt(privilegeCode)) {
				case 0 : privilege.code = PrivilegeCode.READ; break;
				case 1 : privilege.code = PrivilegeCode.READ_WRITE; break;
				case 2 : privilege.code = PrivilegeCode.READ_WRITE_UDF; break;
				case 3 : privilege.code = PrivilegeCode.DATA_ADMIN; break;
				case 4 : privilege.code = PrivilegeCode.SYS_ADMIN; break;
				case 5 : privilege.code = PrivilegeCode.USER_ADMIN; break;
			}
			
			// Create user
			AdminPolicy adminPolicy = new AdminPolicy();
			List<Privilege> privileges = new ArrayList<Privilege>(1);
			privileges.add(privilege);
			
			mClient.revokePrivileges(adminPolicy, roleName, privileges);
			console.printf("\nINFO: The privilege has been revoked from: " + roleName);
		} else {
			console.printf("ERROR: Role not found!");
		}
	}
}