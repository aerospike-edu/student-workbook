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
	//		try {
	fmt.Println("INFO: Connecting to Aerospike cluster...")

	// Establish connection to Aerospike server
	client, err := NewClient("54.90.203.181", 3000)
	panicOnError(err)
	defer client.Close()

	if !client.IsConnected() {
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
			fmt.Print("Enter new password for %s:", username)
			fmt.Scanf("%s", &password)

			writePolicy := NewWritePolicy(0, 0) // generation = 0, expiration = 0
			// record generation
			writePolicy.Generation = userRecord.Generation
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
		// TODO: Create String array of bins you would like to retrieve. In this example, we want to display all tweets for a given user.
		// Exercise 3
		fmt.Printf("\nTODO: Create String array of bins you would like to retrieve. In this example, we want to display all tweets for a given user.")

		// TODO: Create Statement instance
		// Exercise 3
		fmt.Printf("\nTODO: Create Statement instance with: namespace, set and  list of bins you want retrieved on the instance of Statement")

		// TODO: Set equality Filter on username on the instance of Statement
		// Exercise 3
		fmt.Printf("\nTODO: Set equality Filter on the instance of Statement")

		// TODO: Execute query passing in <null> policy and instance of Statement
		// Exercise 3
		fmt.Printf("\nTODO: Execute query passing in <null> policy and instance of Statement")

		// TODO: Iterate through returned RecordSet and output tweets to the console
		// Exercise 3
		fmt.Printf("\nTODO: Iterate through returned RecordSet and output tweets to the console")

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

	// TODO: Create Statement instance
	// Exercise 4
	fmt.Printf("\nTODO: Create Statement instance with: namespace, and list of bins you want retrieved on the instance of Statement")

	// TODO: Set min--max range Filter on tweetcount on the instance of Statement
	// Exercise 4
	fmt.Printf("\nTODO: Set min--max range Filter on the instance of Statement")

	// TODO: Execute query passing in <null> policy and instance of Statement
	// Exercise 4
	fmt.Printf("\nTODO: Execute query passing in null policy and instance of Statement")

	// TODO: Iterate through returned RecordSet and for each record, output text in format "<username> has <#> tweets"
	// Exercise 4
	fmt.Printf("\nTODO: Iterate through returned RecordSet and for each record, output text in format \"<username> has <#> tweets\"")

	// TODO: Close record set
	// Exercise 4
	fmt.Printf("\nTODO: Close record set")
}
