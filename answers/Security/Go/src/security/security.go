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
	"bufio"
	"flag"
	"fmt"
	. "github.com/aerospike/aerospike-client-go"
	"math/rand"
	"os"
	"strings"
	"time"
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
        fmt.Println("6> Create Role\n")
        fmt.Println("7> Read Role\n")
        fmt.Println("8> Grant Privilege to Role\n")
        fmt.Println("9> Revoke Privilege from Role\n")
        fmt.Println("10> Drop Role\n")
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
			case 6:
				fmt.Println("\n********** Your Selection: Create Role **********\n")
				CreateRole(client)
			case 7:
				fmt.Println("\n********** Your Selection: Read Role **********\n")
				ReadRole(client)
			case 8:
				fmt.Println("\n********** Your Selection: Grant Privilege to Role **********\n")
				GrantPrivilege(client)
			case 9:
				fmt.Println("\n********** Your Selection: Revoke Privilege from Role **********\n")
				RevokePrivilege(client)
			case 10:
				fmt.Println("\n********** Your Selection: Drop Role **********\n")
				DropRole(client)				
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
		fmt.Printf("Enter password for %s:", username)
		var password string
		fmt.Scanf("%s", &password)

		// Get role
		fmt.Printf("Enter a role for %s:", username)
		var role string
		fmt.Scanf("%s", &role)

 		var roles := []string{ role }
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
			fmt.Printf("gender:     %s\n", userRoles.Roles)
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
		fmt.Printf("\nINFO: User dropped:\n")
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
		fmt.Printf("Enter a role for %s:", username)
		var role string
		fmt.Scanf("%s", &role)

 		var roles := []string{ role }
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
		fmt.Printf("Enter a role for %s:", username)
		var role string
		fmt.Scanf("%s", &role)

 		var roles := []string{ role }
		err := client.RevokeRoles(nil, username, roles);
		panicOnError(err)
		fmt.Printf("\nINFO: Role revoked\n")
	}
}


func CreateRole(client *Client) {
	fmt.Printf("\n********** Create Role **********\n")

	// Get username
	fmt.Print("Enter role: ")
	var role int
	fmt.Scanf("%s", &role)

	if len(username) > 0 {

		// Get role
		fmt.Printf("Enter a privilege code\n(0 = read, 1 = read-write, 2 = read-write-udf, 3 = data-admin, 4 = sys-admin, 5 = user-admin):", username)
		var role string
		fmt.Scanf("%d", &role)
		
		var privilege = P
		
		switch {
			case role == 0: privilege.code = PrivilegeCode.READ; break;
				case 1 : privilege.code = PrivilegeCode.READ_WRITE; break;
				case 2 : privilege.code = PrivilegeCode.READ_WRITE_UDF; break;
				case 3 : privilege.code = PrivilegeCode.DATA_ADMIN; break;
				case 4 : privilege.code = PrivilegeCode.SYS_ADMIN; break;
				case 5 : privilege.code = PrivilegeCode.USER_ADMIN; break;
			}

 		var roles := []string{ role }
		err := client.RevokeRoles(nil, username, roles);
		panicOnError(err)
		fmt.Printf("\nINFO: Role granted\n")
	}
}

func BatchGetUserTweets(client *Client) {

	// Get username
	var username string
	fmt.Printf("\nEnter username:")
	fmt.Scanf("%s", &username)

	if len(username) > 0 {
		// Check if username exists
		userKey, _ := NewKey("test", "users", username)
		userRecord, err := client.Get(nil, userKey)
		panicOnError(err)

		if userRecord != nil {
			// Get how many tweets the user has
			tweetCount := userRecord.Bins["tweetcount"].(int)

			// Create an array of keys so we can initiate batch read
			// operation
			keys := make([]*Key, tweetCount)

			for i := 0; i < len(keys); i++ {
				keyString, _ := fmt.Scanf("%s:%d", username, i+1)
				key, _ := NewKey("test", "tweets", keyString)
				keys[i] = key
			}

			fmt.Printf("\nHere's %s's tweet(s):\n", username)

			// Initiate batch read operation
			if len(keys) > 0 {
				records, err := client.BatchGet(NewPolicy(), keys)
				panicOnError(err)
				for _, element := range records {
					fmt.Println(element.Bins["tweet"])
				}
			}
		}
	} else {
		fmt.Println("ERROR: User record not found!")
	}
}

func AggregateUsersByTweetCountByRegion(client *Client) {
	var min int64
	var max int64
	fmt.Printf("\nEnter Min Tweet Count:")
	fmt.Scanf("%d", &min)
	fmt.Printf("Enter Max Tweet Count:")
	fmt.Scanf("%d", max)

	fmt.Printf("\nAggregating users with %d - %d tweets by region. Hang on...\n", min, max)

	// NOTE: UDF registration has been included here for convenience and to demonstrate the syntax. The recommended way of registering UDFs in production env is via AQL
	regTask, err := client.RegisterUDFFromFile(nil, "udf/aggregationByRegion.lua", "aggregationByRegion.lua", LUA)
	panicOnError(err)

	// wait until UDF is created
	for {
		if err := <-regTask.OnComplete(); err == nil {
			break
		}
	}

	stmt := NewStatement("test", "users", "tweetcount", "region")
	stmt.Addfilter(NewRangeFilter("tweetcount", min, max))
	
	
	//******************************************************
	//
	//		Aggregations are not yet implemented in Go
	//
	//******************************************************
	

	//			rs, err := us.Client.Query(nil, stmt, "aggregationByRegion", "sum");
	//		panicOnError(err)
	//	L:
	//		for {
	//			select {
	//			case rec, chanOpen := <-rs.Records:
	//				if !chanOpen {
	//					break L
	//				}
	//				fmt.Printf("\nTotal Users in North: %d\n", result["n"]);
	//				fmt.Printf("Total Users in South: %d", result["s"]);
	//				fmt.Printf("Total Users in East: %d", result["e"]);
	//				fmt.Printf("Total Users in West: %d", result["w"]);
	//			case err := <-recordset.Errors:
	//				panicOnError(err)
	//			}
	//		}
	//		rs.Close()
	//

}

//============================================================
// Tweet
//============================================================
func CreateTweets(client *Client) {
	var c string
	randomTweets := []string{
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
		"I dont always tweet, but when I do it is on Tweetaspike"}

	totalUsers := 10000
	maxTweets := 20
	timestamp := 0

	wPolicy := NewWritePolicy(0, 0) // generation = 0, expiration = 0
	wPolicy.RecordExistsAction = UPDATE

	fmt.Printf("Create up to %d tweets each for %d users. Press any key to continue...\n", maxTweets, totalUsers)
	fmt.Scanln("%s", &c)

	for j := 0; j < totalUsers; j++ {
		// Check if user record exists
		username := fmt.Sprintf("user%d", rand.Intn(totalUsers))
		userKey, _ := NewKey("test", "users", username)
		userRecord, err := client.Get(nil, userKey)

		panicOnError(err)
		if userRecord != nil {
			// create up to maxTweets random tweets for this user
			totalTweets := rand.Intn(maxTweets)
			for k := 1; k <= totalTweets; k++ {
				// Create timestamp to store along with the tweet so we can
				// query, index and report on it
				timestamp := getTimeStamp()
				tweetKeyString := fmt.Sprintf("%s:%d", username, k)
				//fmt.Printf("tweet key %s\n", tweetKeyString)
				tweetKey, _ := NewKey("test", "tweets", tweetKeyString)
				bin1 := NewBin("tweet", randomTweets[rand.Intn(len(randomTweets))])
				bin2 := NewBin("ts", timestamp)
				bin3 := NewBin("username", username)

				err := client.PutBins(wPolicy, tweetKey, bin1, bin2, bin3)
				panicOnError(err)
			}
			fmt.Printf("Wrote %d tweets for %s!\n", totalTweets, username)
			if totalTweets > 0 {
				// Update tweet count and last tweet'd timestamp in the user
				// record
				err := client.PutBins(wPolicy, userKey, NewBin("tweetcount", totalTweets), NewBin("lasttweeted", timestamp))
				panicOnError(err)
			}
		}
	}
	fmt.Printf("\nDone creating up to %d tweets each for %d users!\n", maxTweets, totalUsers)
}

func getTimeStamp() int64 {
	now := time.Now()
	return now.Unix()
}

func CreateTweet(client *Client) {
	in := bufio.NewReader(os.Stdin)
	fmt.Println("\n********** Create Tweet **********")

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

	// Get username
	var username string
	fmt.Print("\nEnter username:")
	fmt.Scanf("%s", &username)

	if len(username) > 0 {
		// Check if username exists
		userKey, _ := NewKey("test", "users", username)
		userRecord, err := client.Get(nil, userKey)
		panicOnError(err)
		if userRecord != nil {
			tweetCount := userRecord.Bins["tweetcount"].(int) + 1

			// Get tweet
			fmt.Printf("Enter tweet for %s:", username)
			tweet, _ := in.ReadString('\n')

			// Write record
			wPolicy := NewWritePolicy(0, 0) // generation = 0, expiration = 0
			wPolicy.RecordExistsAction = UPDATE

			// Create timestamp to store along with the tweet so we can
			// query, index and report on it
			timestamp := getTimeStamp()

			keyString := fmt.Sprintf("%s:%d", username, tweetCount)
			tweetKey, _ := NewKey("test", "tweets", keyString)
			bin1 := NewBin("tweet", tweet)
			bin2 := NewBin("ts", timestamp)
			bin3 := NewBin("username", username)

			err := client.PutBins(wPolicy, tweetKey, bin1, bin2, bin3)
			panicOnError(err)
			fmt.Printf("\nINFO: Tweet record created! with key: %s, %v, %v, %v\n", keyString, bin1, bin2, bin3)

			// Update tweet count and last tweet'd timestamp in the user
			// record
			updateUser(client, userKey, nil, timestamp, tweetCount)
		} else {
			fmt.Println("ERROR: User record not found!")
		}
	}
}

func updateUser(client *Client, userKey *Key,
	policy *WritePolicy, timestamp int64, tweetCount int) {
	bin1 := NewBin("tweetcount", int64(tweetCount))
	bin2 := NewBin("lasttweeted", timestamp)
	err := client.PutBins(policy, userKey, bin1, bin2)
	panicOnError(err)
	fmt.Printf("\nINFO: The tweet count now is: %v\n", bin1)

	// updateUserUsingOperate(userKey, policy, ts);
}

func updateUserUsingOperate(client *Client, userKey *Key,
	policy *WritePolicy, timestamp int64) {

	record, err := client.Operate(policy, userKey,
		AddOp(NewBin("tweetcount", 1)),
		PutOp(NewBin("lasttweeted", timestamp)),
		GetOp())
	panicOnError(err)

	fmt.Printf("\nINFO: The tweet count now is: %d\n", record.Bins["tweetcount"])
}

func ScanAllTweetsForAllUsers(client *Client) {
	policy := NewScanPolicy()
	policy.ConcurrentNodes = true
	policy.Priority = LOW
	policy.IncludeBinData = true

	records, err := client.ScanAll(policy, "test", "tweets", "tweet")
	panicOnError(err)

	for element := range records.Records {
		fmt.Println(element.Bins["tweet"])
	}

}

func QueryTweets(client *Client) {
	queryTweetsByUsername(client)
	queryUsersByTweetCount(client)
}

func queryTweetsByUsername(client *Client) {

	fmt.Printf("\n********** Query Tweets By Username **********\n")


	// Get username
	var username string
	fmt.Printf("\nEnter username:")
	fmt.Scanln(&username)

	if len(username) > 0 {
		stmt := NewStatement("test", "tweets", "tweet")
		stmt.Addfilter(NewEqualFilter("username", username))

		fmt.Printf("\nHere's " + username + "'s tweet(s):\n")

		recordset, err := client.Query(nil, stmt)
		panicOnError(err)
	L:
		for {
			select {
			case rec, chanOpen := <-recordset.Records:
				if !chanOpen {
					break L
				}
				fmt.Println(rec.Bins["tweet"])
			case err := <-recordset.Errors:
				panicOnError(err)
			}
		}
		recordset.Close()

	} else {
		fmt.Printf("ERROR: User record not found!\n")
	}
}

func queryUsersByTweetCount(client *Client) {

	fmt.Printf("\n********** Query Users By Tweet Count Range **********\n")


	// Get min and max tweet counts
	var min int64
	var max int64
	fmt.Printf("\nEnter Min Tweet Count:")
	fmt.Scanf("%d", &min)
	fmt.Printf("Enter Max Tweet Count:")
	fmt.Scanf("%d", &max)

	fmt.Printf("\nList of users with %d - %d tweets:\n", min, max)

	stmt := NewStatement("test", "users", "username", "tweetcount", "gender")
	stmt.Addfilter(NewRangeFilter("tweetcount", min, max))

	recordset, err := client.Query(nil, stmt)
	panicOnError(err)
L:
	for {
		select {
		case rec, chanOpen := <-recordset.Records:
			if !chanOpen {
				break L
			}
			fmt.Printf("%s has %d tweets\n", rec.Bins["username"], rec.Bins["tweetcount"])
		case err := <-recordset.Errors:
			panicOnError(err)
		}
	}
	recordset.Close()
}
