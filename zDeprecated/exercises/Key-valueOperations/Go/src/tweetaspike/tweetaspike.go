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
//	"strings"
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
	//		try {
	fmt.Println("INFO: Connecting to Aerospike cluster...")

	// Establish connection to Aerospike server
	var client* Client
	// TODO: Establish a connection to Aerospike cluster
	// Exercise 1

	// TODO: Close Aerospike cluster connection -- HINT: use defer
	// Exercise 1

	// TODO: Check to see if the cluster connection succeeded
	// Exercise 1
	fmt.Println("\nTODO: Check to see if the cluster connection succeeded")

	if false {
		fmt.Println("ERROR: Connection to Aerospike cluster failed! Please check the server settings and try again!")
		fmt.Scanf("%s", &c)

	} else {
		fmt.Println("INFO: Connection to Aerospike cluster succeeded!")

		var feature int
		// Present options
		fmt.Println("What would you like to do:")
		fmt.Println("1> Create A User And A Tweet")
		fmt.Println("2> Read A User Record")
		fmt.Println("3> Batch Read Tweets For A User")
		fmt.Println("4> Scan All Tweets For All Users")
		fmt.Println("5> Record UDF -- Update User Password")
		fmt.Println("6> Query Tweets By Username And Users By Tweet Count Range")
		fmt.Println("7> Stream UDF -- Aggregation Based on Tweet Count By Region")
		fmt.Println("0> Exit")
		fmt.Println("\nSelect 0-7 and hit enter:")
		fmt.Scanf("%d", &feature)

		if feature != 0 {
			switch feature {
			case 1:
				fmt.Println("\n********** Your Selection: Create User And A Tweet **********\n")
				CreateUser(client)
				CreateTweet(client)
			case 2:
				fmt.Println("\n********** Your Selection: Read A User Record **********\n")
				GetUser(client)
			case 3:
				fmt.Println("\n********** Your Selection: Batch Read Tweets For A User **********\n")
				BatchGetUserTweets(client)
			case 4:
				fmt.Println("\n********** Your Selection: Scan All Tweets For All Users **********\n")
				ScanAllTweetsForAllUsers(client)
			case 5:
				fmt.Println("\n********** Your Selection: Record UDF -- Update User Password **********\n")
				UpdatePasswordUsingUDF(client)
				//UpdatePasswordUsingCAS(client);
			case 6:
				fmt.Println("\n********** Your Selection: Query Tweets By Username And Users By Tweet Count Range **********\n")
				QueryTweets(client)
			case 7:
				fmt.Println("\n********** Your Selection: Stream UDF -- Aggregation Based on Tweet Count By Region **********\n")
				AggregateUsersByTweetCountByRegion(client)
			case 12:
				fmt.Println("\n********** Create Users **********\n")
				CreateUsers(client)
			case 23:
				fmt.Println("\n********** Create Tweets **********\n")
				CreateTweets(client)
			default:
			}
		}
	}

	fmt.Println("\n\nINFO: Press any key to exit...\n")
	fmt.Scanf("%s", &c)
}

func CreateUsers(client *Client) {
	var c string
	genders := []string{"m", "f"}
	regions := []string{"n", "s", "e", "w"}
	randomInterests := []string{"Music", "Football", "Soccer", "Baseball", "Basketball", "Hockey", "Weekend Warrior", "Hiking", "Camping", "Travel", "Photography"}
	var userInterests []string
	totalInterests := 0
	start := 1
	end := 100000
	totalUsers := end - start

	wPolicy := NewWritePolicy(0, 0) // generation = 0, expiration = 0
	wPolicy.RecordExistsAction = UPDATE

	fmt.Printf("Create %d users. Press any key to continue...\n", totalUsers)
	fmt.Scanf("%s", &c)

	for j := start; j <= end; j++ {
		userInterests = []string{}
		// Write user record
		username := fmt.Sprintf("user%d", j)
		key, _ := NewKey("test", "users", username)
		bin1 := NewBin("username", fmt.Sprintf("user%d", j))
		bin2 := NewBin("password", fmt.Sprintf("pwd%d", j))
		bin3 := NewBin("gender", genders[rand.Intn(2)])
		bin4 := NewBin("region", regions[rand.Intn(4)])
		bin5 := NewBin("lasttweeted", 0)
		bin6 := NewBin("tweetcount", 0)

		totalInterests = rand.Intn(7)
		for i := 0; i < totalInterests; i++ {
			userInterests = append(userInterests, randomInterests[rand.Intn(len(randomInterests))])
		}
		bin7 := NewBin("interests", userInterests)

		err := client.PutBins(wPolicy, key, bin1, bin2, bin3, bin4, bin5, bin6, bin7)
		panicOnError(err)
		fmt.Printf("Wrote user record for %s: %v\n", username, userInterests)
	}
	fmt.Printf("Done creating %d!\n", totalUsers)

}

func CreateUser(client *Client) {
	fmt.Printf("\n********** Create User **********\n")

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

	// Get username
	fmt.Print("Enter username: ")
	var username string
	fmt.Scanf("%s", &username)

	if len(username) > 0 {
		// Get password
		fmt.Printf("Enter password for %s:", username)
		var password string
		fmt.Scanf("%s", &password)

		// Get gender
		fmt.Printf("Select gender (f or m) for %s:", username)
		var gender string
		fmt.Scanf("%s", &gender)

		// Get region
		fmt.Printf("Select region (north, south, east or west) for %s:", username)
		var region string
		fmt.Scanf("%s", &region)

		// Get interests
		fmt.Printf("Enter comma-separated interests for %s:", username)
		var interests string
		fmt.Scanf("%s", &interests)

		// TODO: Create WritePolicy instance
		// Exercise 2
		fmt.Printf("\nTODO: Create WritePolicy instance")

		// TODO: Create Key and Bin instances for the user record. Remember to convert comma-separated interests into a list before storing it.
		// Exercise 2
		fmt.Printf("\nTODO: Create Key and Bin instances for the user record. Remember to convert comma-separated interests into a list before storing it.")

		// TODO: Write user record
		// Exercise 2
		fmt.Printf("\nTODO: Write user record")

	}
}

func GetUser(client *Client) {

	// Get username
	var username string
	fmt.Print("Enter username:")
	fmt.Scanf("%s", &username)

	if len(username) > 0 {
		var userRecord* Record
		// Check if username exists

		// TODO: Read user record
		// Exercise 2
		if userRecord != nil {
			// TODO: Output user record to the console. Remember to convert comma-separated interests into a list before outputting it.
			// Exercise 2
			fmt.Printf("\nTODO: Output user record to the console. Remember to convert comma-separated interests into a list before outputting it")
		} else {
			fmt.Printf("ERROR: User record not found!\n")
		}
	} else {
		fmt.Printf("ERROR: User record not found!\n")
	}
}

func UpdatePasswordUsingUDF(client *Client) {

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
			// Get new password
			var password string
			fmt.Printf("Enter new password for %s:", username)
			fmt.Scanf("%s", &password)

			// NOTE: UDF registration has been included here for convenience and to demonstrate the syntax. The recommended way of registering UDFs in production env is via AQL

			regTask, err := client.RegisterUDFFromFile(nil, "udf/updateUserPwd.lua", "updateUserPwd.lua", LUA)
			panicOnError(err)

			// wait until UDF is created
			for {
				if err := <-regTask.OnComplete(); err == nil {
					break
				}
			}

			updatedPassword, err := client.Execute(nil, userKey, "updateUserPwd", "updatePassword", NewValue(password))
			panicOnError(err)
			fmt.Printf("\nINFO: The password has been set to: %s\n", updatedPassword)
		} else {
			fmt.Printf("ERROR: User record not found!\n")
		}
	} else {
		fmt.Printf("ERROR: User record not found!\n")
	}
}

func UpdatePasswordUsingCAS(client *Client) {

	// Get username
	var username string
	fmt.Print("Enter username:")
	fmt.Scanf("%s", &username)

	if len(username) > 0 {
		// Check if username exists
		userKey, _ := NewKey("test", "users", username)
		_, err := client.Get(nil, userKey)
		//userRecord, err := client.Get(nil, userKey)
		panicOnError(err)
		if err == nil {
			// Get new password
			var password string
			fmt.Print("Enter new password for %s:", username)
			fmt.Scanf("%s", &password)
			

			// TODO: Update User record with new password only if generation is the same
			// Exercise 5
			fmt.Printf("\nINFO: The password has been set to: %s", password)
		} else {
			fmt.Printf("ERROR: User record not found!")
		}
	} else {
		fmt.Printf("ERROR: User record not found!")
	}
}

func BatchGetUserTweets(client *Client) {

	// Get username
	var username string
	fmt.Printf("\nEnter username:")
	fmt.Scanf("%s", &username)

	if len(username) > 0 {
		var userRecord* Record
		// TODO: Read user record
		// Exercise 3
		fmt.Printf("\nTODO: Read user record")

		if userRecord != nil {
			// TODO: Get how many tweets the user has
			// Exercise 3
			fmt.Println("\nTODO: Get how many tweets the user has")

			// TODO: Create an array of tweet keys -- keys[tweetCount]
			// Exercise 3
			fmt.Println("\nTODO: Create an array of Key instances -- keys[tweetCount]")

			// TODO: Initiate batch read operation
			// Exercise 3
			fmt.Println("\nTODO: Initiate batch read operation")

			// TODO: Output tweets to the console
			// Exercise 3
			fmt.Println("\nTODO: Output tweets to the console")
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
	// TODO: Update tweet count and last tweeted timestamp in the user record
	// Exercise 2
	fmt.Printf("\nTODO: Update tweet count and last tweeted timestamp in the user record")

	// TODO: Update tweet count and last tweeted timestamp in the user record using Operate
	// Exercise 6
	// updateUserUsingOperate(userKey, policy, ts);
}

func updateUserUsingOperate(client *Client, userKey *Key,
	policy *WritePolicy, timestamp int64) {

	// TODO: Initiate operate passing in policy, user record key, .add operation incrementing tweet count, .put operation updating timestamp and .get operation to read the user record
	// Exercise 6
	fmt.Println("\nTODO: Initiate operate passing in policy, user record key, .add operation incrementing tweet count, .put operation updating timestamp and .get operation to read the user record")

	// TODO: Output most recent tweet count
	// Exercise 6
	fmt.Println("\nTODO: Output most recent tweet count")

}

func ScanAllTweetsForAllUsers(client *Client) {
	// TODO: Create ScanPolicy instance
	// Exercise 4
	fmt.Println("\nTODO: Create ScanPolicy instance")
	// TODO: Set policy parameters (optional)
	// Exercise 4
	fmt.Println("\nTODO: Set policy parameters (optional)")
	// TODO: Initiate scan operation that invokes callback for outputting tweets on the console
	// Exercise 4
	fmt.Println("\nTODO: Initiate scan operation that invokes callback for outputting tweets to the console")
}

func QueryTweets(client *Client) {
	queryTweetsByUsername(client)
	queryUsersByTweetCount(client)
}

func queryTweetsByUsername(client *Client) {

	fmt.Printf("\n********** Query Tweets By Username **********\n")

	// NOTE: Index creation has been included in here for convenience and to demonstrate the syntax.
	// NOTE: The recommended way of creating indexes in production env is via AQL.
	//			IndexTask task = client.createIndex(null, "test", "tweets",
	//					"username_index", "username", IndexType.STRING);
	//			task.waitTillComplete(100);

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

	// NOTE: Index creation has been included in here for convenience and to demonstrate the syntax.
	// NOTE: The recommended way of creating indexes in production env is via AQL.
	//			IndexTask task = client.createIndex(null, "test", "users",
	//					"tweetcount_index", "tweetcount", IndexType.NUMERIC);
	//			task.waitTillComplete(100);

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
