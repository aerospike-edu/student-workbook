/* 
 * Copyright 2012-2020 Aerospike, Inc.
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
        // Exercise K1 - add your IP address
	client, err := NewClient("127.0.0.1", 3000) //K1 - add IP address
	panicOnError(err)
	defer client.Close()  //K1 - close client

	if !client.IsConnected() {
		fmt.Println("ERROR: Connection to Aerospike cluster failed! Please check the server settings and try again!")
		fmt.Scanf("%s", &c)

	} else {
		fmt.Println("INFO: Connection to Aerospike cluster succeeded!")

		var feature int
		// Present options
		fmt.Println("What would you like to do:")
		fmt.Println("1> Create A User")
		fmt.Println("2> Create A Tweet by a User")
		fmt.Println("3> Read A User Record")
		fmt.Println("4> Batch Read Tweets For A User")
		fmt.Println("5> Scan All Tweets For All Users")
		fmt.Println("6> Update User Password using CAS")
		fmt.Println("7> Record UDF -- Update User Password")
		fmt.Println("8> Query Tweets By Username")
		fmt.Println("9> Query Tweets By Tweet Count Range")
		fmt.Println("10> Stream UDF -- Aggregation Based on Tweet Count By Region")
		fmt.Println("11> Create a Test Set of Users")
		fmt.Println("12> Create a Test Set of Tweets")
		fmt.Println("0> Exit")
		fmt.Println("\nSelect 0-12 and hit enter:")
		fmt.Scanf("%d", &feature)

		if feature != 0 {
			switch feature {
			case 1:
				fmt.Println("\n********** Your Selection: Create A User**********\n")
				CreateUser(client)
			case 2:
				fmt.Println("\n********** Your Selection: Create a User's Tweet **********\n")
				CreateTweet(client)
			case 3:
				fmt.Println("\n********** Your Selection: Read A User Record **********\n")
				GetUser(client)
			case 4:
				fmt.Println("\n********** Your Selection: Batch Read Tweets For A User **********\n")
				BatchGetUserTweets(client)
			case 5:
				fmt.Println("\n********** Your Selection: Scan All Tweets For All Users **********\n")
				ScanAllTweetsForAllUsers(client)
			case 6:
				fmt.Println("\n********** Your Selection: Record UDF -- Update User Password **********\n")
				UpdatePasswordUsingUDF(client)
			case 7:
				fmt.Println("\n********** Your Selection: Update User Password using CAS **********\n")
				UpdatePasswordUsingCAS(client);
			case 8:
				fmt.Println("\n********** Your Selection: Query Tweets By Username **********\n")
	                        queryTweetsByUsername(client)
			case 9:
				fmt.Println("\n********** Your Selection: Query Tweets By Tweet Count Range **********\n")
	                        queryUsersByTweetCount(client)
			case 10:
				fmt.Println("\n********** Your Selection: Stream UDF -- Aggregation Based on Tweet Count By Region **********\n")
				AggregateUsersByTweetCountByRegion(client)
			case 11:
				fmt.Println("\n********** Create Users **********\n")
				CreateUsers(client)
			case 12:
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
	start := 0
	end := 9999
	totalUsers := end - start

	wPolicy := NewWritePolicy(0, 0) // generation = 0, expiration = 0
	wPolicy.RecordExistsAction = UPDATE

	fmt.Printf("Create %d users. (user0 thru user%d) Press any key to continue...\n", totalUsers+1, totalUsers)
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

		// Write record
		wPolicy := NewWritePolicy(0, 0) // generation = 0, expiration = 0
		wPolicy.RecordExistsAction = UPDATE

		key, _ := NewKey("test", "users", username)
		bin1 := NewBin("username", username)
		bin2 := NewBin("password", password)
		bin3 := NewBin("gender", gender)
		bin4 := NewBin("region", region)
		bin5 := NewBin("lasttweeted", 0)
		bin6 := NewBin("tweetcount", 0)
		arr := strings.Split(interests, ",")
		bin7 := NewBin("interests", arr)

		err := client.PutBins(wPolicy, key, bin1, bin2, bin3, bin4, bin5, bin6, bin7)
		panicOnError(err)
		fmt.Printf("\nINFO: User record created!\n")
	}
}

func GetUser(client *Client) {

	// Get username
	var username string
	fmt.Print("Enter username:")
	fmt.Scanf("%s", &username)

	if len(username) > 0 {
		// Check if username exists
		userKey, _ := NewKey("test", "users", username)
		userRecord, err := client.Get(nil, userKey)
		panicOnError(err)
		if userRecord != nil {
			fmt.Printf("\nINFO: User record read successfully! Here are the details:\n")
			fmt.Printf("username:   %s\n", userRecord.Bins["username"].(string))
			fmt.Printf("password:   %s\n", userRecord.Bins["password"].(string))
			fmt.Printf("gender:     %s\n", userRecord.Bins["gender"].(string))
			fmt.Printf("region:     %s\n", userRecord.Bins["region"].(string))
			fmt.Printf("tweetcount: %d\n", userRecord.Bins["tweetcount"].(int))
			fmt.Printf("interests:  %v\n", userRecord.Bins["interests"])
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
		userRecord, err := client.Get(nil, userKey)
		panicOnError(err)
		if err == nil {
			// Get new password
			var password string
			fmt.Printf("Enter new password for %s:", username)
			fmt.Scanf("%s", &password)

			writePolicy := NewWritePolicy(0, 0) // generation = 0, expiration = 0
			// record generation
			writePolicy.Generation = uint32(userRecord.Generation)
			writePolicy.GenerationPolicy = EXPECT_GEN_EQUAL
			// password Bin
			passwordBin := NewBin("password", password)
			err = client.PutBins(writePolicy, userKey, passwordBin)
			panicOnError(err)
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
		// Check if username exists
		userKey, _ := NewKey("test", "users", username)
		userRecord, err := client.Get(nil, userKey)
		panicOnError(err)

		if userRecord != nil {
			// Get how many tweets the user has
			tweetCount := userRecord.Bins["tweetcount"].(int)
                        fmt.Printf("User %s has %d tweets\n",username,tweetCount)

			// Create an array of keys so we can initiate batch read
			// operation
			keys := make([]*Key, tweetCount)

			for i := 0; i < len(keys); i++ {
				keyString := fmt.Sprintf("%s:%d", username, i+1)
				key, _ := NewKey("test", "tweets", keyString)
				keys[i] = key
			}

			fmt.Printf("\nHere's %s's tweet(s):\n", username)

			// Initiate batch read operation

			if len(keys) > 0 {
				records, err := client.BatchGet(NewBatchPolicy(), keys)
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
	var min int64  //No int64 ... LUA has only 56 bits for int
	var max int64
	fmt.Printf("\nEnter Min Tweet Count:")
	fmt.Scanf("%d", &min)
	fmt.Printf("Enter Max Tweet Count:")
	fmt.Scanf("%d", &max)

	fmt.Printf("\nAggregating users with %d - %d tweets by region. Hang on...\n", min, max)

	// NOTE: UDF registration has been included here for convenience and 
        // to demonstrate the syntax. The recommended way of registering UDFs 
        // in production env is via AQL

        //Set LuaPath
        luaPath, _ := os.Getwd()
        luaPath += "/udf/"
        SetLuaPath(luaPath)


	// Load UDF and wait until UDF is created

	regTask, err := client.RegisterUDFFromFile(nil, luaPath+"aggregationByRegion.lua", "aggregationByRegion.lua", LUA)
	panicOnError(err)

	for {
		if err := <-regTask.OnComplete(); err == nil {
		  break
		}
	}

	stmt := NewStatement("test", "users", "tweetcount", "region")
	stmt.SetFilter(NewRangeFilter("tweetcount", min, max))  //SI on tweetcount required

	rs, err := client.QueryAggregate(nil, stmt, "aggregationByRegion", "sum");
	panicOnError(err)

        for rec := range rs.Results() {
          if(rec != nil){
              res := rec.Record.Bins["SUCCESS"].(map[interface{}]interface{})
              fmt.Printf("\nTotal Users in North: %.0f\n", res["n"]);
              fmt.Printf("Total Users in South: %.0f\n", res["s"]);
              fmt.Printf("Total Users in East: %.0f\n", res["e"]);
              fmt.Printf("Total Users in West: %.0f\n", res["w"]);
          }
         }

         rs.Close()
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
		"If you are thirsty, don't have soda.",
		"Hear that snap? Hear that clap?",
		"Follow me and I may follow you",
		"Which is the best cafe in Portland? Discuss...",
		"Portland Coffee is for closers!",
		"Lets get this party started!",
		"How about them portland blazers!", "You got school'd, yo",
		"I love animals", "I love my dog", "What's up Portland",
		"Which is the best cafe in Portland? Discuss...",
		"I dont always tweet, but when I do it is on Tweetaspike"}

	totalUsers := 1000
	maxUsers := 10000
	maxTweets := 20
	timestamp := 0

	wPolicy := NewWritePolicy(0, 0) // generation = 0, expiration = 0
	wPolicy.RecordExistsAction = UPDATE

	fmt.Printf("Create up to %d tweets each for %d users. Press any key to continue...\n", maxTweets, totalUsers)
	fmt.Scanf("%s", &c)

	for j := 0; j < totalUsers; j++ {
		// Check if user record exists
		username := fmt.Sprintf("user%d", rand.Intn(maxUsers))
		userKey, _ := NewKey("test", "users", username)
		userRecord, err := client.Get(nil, userKey)

                fmt.Printf("username=%s, ", username)

		panicOnError(err)
		if userRecord != nil {
			// create up to maxTweets random tweets for this user
			totalTweets := rand.Intn(maxTweets)
                        if(totalTweets==0) {
                          totalTweets = 1  //Every user selected should get one tweet at least
                        }
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
			fmt.Printf("Wrote %d tweets for %s\n", totalTweets, username)
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
			timestamp := getTimeStamp()  //Note: LUA will only store 56 bits

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

	// updateUserUsingOperate(client, userKey, policy, timestamp);
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

        //Exercise K5 - add code for all locations marked "x" //K5
	policy := x //K5
	policy.x= true //K5
	policy.x= LOW //K5
	policy.x= true //K5

	records, err := client.ScanAll(x) //K5
	panicOnError(err)

	for element := range records.Records {
		fmt.Println(x) //K5
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
		stmt.SetFilter(NewEqualFilter("username", username))

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
	stmt.SetFilter(NewRangeFilter("tweetcount", min, max))

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
