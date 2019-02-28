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
package main

import (
	"flag"
	"fmt"
	. "github.com/aerospike/aerospike-client-go"
)

const APP_VERSION = "1.0"

// The flag package provides a default help printer via -h switch
var versionFlag *bool = flag.Bool("v", false, "Print the version number.")

func panicOnError(err error) {
	if err != nil {
		fmt.Printf("Aerospike error: %d", err)
		panic(err)
	}
}

func main() {
	var c string
	flag.Parse() // Scan the arguments list

	if *versionFlag {
		fmt.Println("Version:", APP_VERSION)
	}
	fmt.Println("***** Welcome to Aerospike Developer Training *****\n")
	
	fmt.Println("INFO: Connecting to Aerospike cluster...")
	// Establish connection to Aerospike server
	clientPolicy := NewClientPolicy()
	clientPolicy.User = "superman"
	clientPolicy.Password = "krypton"
	client, err := NewClientWithPolicy(clientPolicy, "127.0.0.1", 3000)
	panicOnError(err)
	defer client.Close()

	if !client.IsConnected() {
		fmt.Println("ERROR: Connection to Aerospike cluster failed! Please check the server settings and try again!")
		fmt.Scanf("%s", &c)

	} else {
		fmt.Println("INFO: Connection to Aerospike cluster succeeded!")

		var feature int
		// Present options
        fmt.Println("\nWhat would you like to do:\n")
        fmt.Println("1> Create User\n")
        fmt.Println("2> Read User\n")
        fmt.Println("3> Grant Role to User\n")
        fmt.Println("4> Revoke Role from User\n")
        fmt.Println("5> Drop User\n")
        fmt.Println("0> Exit\n")
        fmt.Println("\nSelect 0-10 and hit enter:\n")
		fmt.Scanf("%d", &feature)

		if feature != 0 {
			switch feature {
			case 1:
				fmt.Println("\n********** Your Selection: Create User **********\n")
				CreateUser(client)
			case 2:
				fmt.Println("\n********** Your Selection: Read User **********\n")
				GetUser(client)
			case 3:
				fmt.Println("\n********** Your Selection: Grant Role to User **********\n")
				GrantRole(client)
			case 4:
				fmt.Println("\n********** Your Selection: Revoke Role from User **********\n")
				RevokeRole(client)
			case 5:
				fmt.Println("\n********** Your Selection: Drop User **********\n")
				DropUser(client)				
			default:
			}
		}
	}

	fmt.Println("\n\nINFO: Press any key to exit...\n")
	fmt.Scanf("%s", &c)
}

func CreateUser(client *Client) {
	fmt.Printf("\n********** Create User **********\n")

	// Get username
	fmt.Print("Enter username: ")
	var username string
	fmt.Scanf("%s", &username)

	if len(username) > 0 {
		// Get password
		fmt.Printf("Enter password for %s: ", username)
		var password string
		fmt.Scanf("%s", &password)

		// Get role
		fmt.Printf("Enter a role for %s: ", username)
		var role string
		fmt.Scanf("%s", &role)

 		roles := []string{role}

		err := client.CreateUser(nil, username, password, roles);
		panicOnError(err)
		fmt.Printf("\nINFO: User created!\n")
	}
}

func GetUser(client *Client) {

	// Get username
	var username string
	fmt.Print("Enter username:")
	fmt.Scanf("%s", &username)

	if len(username) > 0 {
		userRoles, err := client.QueryUser(nil, username)
		panicOnError(err)
		if userRoles != nil {
			fmt.Printf("\nINFO: User read successfully! Here are the details:\n")
			fmt.Printf("roles:     %s\n", userRoles.Roles)
		} else {
			fmt.Printf("ERROR: User not found!\n")
		}
	} else {
		fmt.Printf("ERROR: User not found!\n")
	}
}

func DropUser(client *Client) {

	// Get username
	var username string
	fmt.Print("Enter username:")
	fmt.Scanf("%s", &username)

	if len(username) > 0 {
		client.DropUser(nil, username)
		fmt.Printf("\nINFO: User dropped\n")
	} else {
		fmt.Printf("ERROR: User not found!\n")
	}
}

func GrantRole(client *Client) {
	fmt.Printf("\n********** Grant Role **********\n")

	// Get username
	fmt.Print("Enter username: ")
	var username string
	fmt.Scanf("%s", &username)

	if len(username) > 0 {

		// Get role
		fmt.Printf("Enter a role for %s: ", username)
		var role string
		fmt.Scanf("%s", &role)

 		roles := []string{role}
		err := client.GrantRoles(nil, username, roles);
		panicOnError(err)
		fmt.Printf("\nINFO: Role granted\n")
	}
}


func RevokeRole(client *Client) {
	fmt.Printf("\n********** Revoke Role **********\n")

	// Get username
	fmt.Print("Enter username: ")
	var username string
	fmt.Scanf("%s", &username)

	if len(username) > 0 {

		// Get role
		fmt.Printf("Enter a role to revoke from %s: ", username)
		var role string
		fmt.Scanf("%s", &role)

 		roles := []string{role}
		err := client.RevokeRoles(nil, username, roles);
		panicOnError(err)
		fmt.Printf("\nINFO: Role revoked\n")
	}
}
