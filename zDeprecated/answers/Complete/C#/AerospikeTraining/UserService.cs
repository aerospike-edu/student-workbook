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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Aerospike.Client;
using System.IO;

namespace AerospikeTraining
{
    class UserService
    {
        private AerospikeClient client;

        public UserService(AerospikeClient c)
        {
            this.client = c;
        }

        public void createUser()
        {
            Console.WriteLine("\n********** Create User **********\n");

            ///*********************///
            ///*****Data Model*****///
            //Namespace: test
            //Set: Users
                //Key: <username>
                //Bins:
                    //username - string
                    //password - string (For simplicity password is stored in plain-text)
                    //gender - string (Valid values are 'm' or 'f')
                    //region - string (Valid values are: 'n' (North), 's' (South), 'e' (East), 'w' (West) -- to keep data entry to minimal we just store the first letter)
                    //lasttweeted - Integer (Stores epoch timestamp of the last/most recent tweet) -- Default to 0
                    //tweetcount - Integer (Stores total number of tweets for the user) -- Default to 0
                    //interests - Array of interests) 

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

            string username;
            string password;
            string gender;
            string region;
            string interests;

            // Get username
            Console.WriteLine("Enter username: ");
            username = Console.ReadLine();

			if (username != null && username.Length > 0)
            {
                // Get password
                Console.WriteLine("Enter password for " + username + ":");
                password = Console.ReadLine();

                // Get gender
                Console.WriteLine("Select gender (f or m) for " + username + ":");
                gender = Console.ReadLine().Substring(0, 1);

                // Get region
                Console.WriteLine("Select region (north, south, east or west) for " + username + ":");
                region = Console.ReadLine().Substring(0, 1);

                // Get interests
                Console.WriteLine("Enter comma-separated interests for " + username + ":");
                interests = Console.ReadLine();

                // Write record
                WritePolicy wPolicy = new WritePolicy();
                wPolicy.recordExistsAction = RecordExistsAction.UPDATE;

                Key key = new Key("test", "users", username);
                Bin bin1 = new Bin("username", username);
                Bin bin2 = new Bin("password", password);
                Bin bin3 = new Bin("gender", gender);
                Bin bin4 = new Bin("region", region);
                Bin bin5 = new Bin("lasttweeted", 0);
                Bin bin6 = new Bin("tweetcount", 0);
                Bin bin7 = Bin.AsList("interests", interests.Split(',').ToList<object>());

                client.Put(wPolicy, key, bin1, bin2, bin3, bin4, bin5, bin6, bin7);

                Console.WriteLine("\nINFO: User record created!");
            }
        } //createUser

        public void getUser()
        {
            Record userRecord = null;
            Key userKey = null;

            // Get username
            string username;
            Console.WriteLine("\nEnter username:");
            username = Console.ReadLine();

			if (!string.IsNullOrEmpty (username)) {
				// Check if username exists
				userKey = new Key ("test", "users", username);
				userRecord = client.Get (null, userKey);
				if (userRecord != null) {
					Console.WriteLine ("\nINFO: User record read successfully! Here are the details:\n");
					Console.WriteLine ("username:     " + userRecord.GetValue ("username"));
					Console.WriteLine ("password:     " + userRecord.GetValue ("password"));
					Console.WriteLine ("gender:       " + userRecord.GetValue ("gender"));
					Console.WriteLine ("region:       " + userRecord.GetValue ("region"));
					Console.WriteLine ("tweetcount:   " + userRecord.GetValue ("tweetcount"));
					List<object> interests = (List<object>)userRecord.GetValue ("interests");
					Console.WriteLine ("interests:    " + interests.Aggregate ((x, y) => x + "," + y));
				} else {
					Console.WriteLine ("ERROR: User record not found!");
				}
			} else {
				Console.WriteLine ("ERROR: User record not found!");
			}
        } //getUser

        public void updatePasswordUsingUDF()
        {
            Record userRecord = null;
            Key userKey = null;

            // Get username
            string username;
            Console.WriteLine("\nEnter username:");
            username = Console.ReadLine();

			if (!string.IsNullOrEmpty (username)) {
				// Check if username exists
				userKey = new Key ("test", "users", username);
				userRecord = client.Get (null, userKey);
				if (userRecord != null) {
					// Get new password
					string password;
					Console.WriteLine ("Enter new password for " + username + ":");
					password = Console.ReadLine ();

					// NOTE: UDF registration has been included here for convenience and to demonstrate the syntax. The recommended way of registering UDFs in production env is via AQL
					string luaDirectory = @"..\..\udf";
					LuaConfig.PackagePath = luaDirectory + @"\?.lua";
					string filename = "updateUserPwd.lua";
					string path = Path.Combine (luaDirectory, filename);
					RegisterTask rt = client.Register (null, path, filename, Language.LUA);
					rt.Wait ();

					string updatedPassword = client.Execute (null, userKey, "updateUserPwd", "updatePassword", Value.Get (password)).ToString ();
					Console.WriteLine ("\nINFO: The password has been set to: " + updatedPassword);
				} else {
					Console.WriteLine ("ERROR: User record not found!");
				}
			} else {
				Console.WriteLine ("ERROR: User record not found!");
			}
        } //updatePasswordUsingUDF

        public void updatePasswordUsingCAS()
        {
            Record userRecord = null;
            Key userKey = null;
            Bin passwordBin = null;

            // Get username
            string username;
            Console.WriteLine("\nEnter username:");
            username = Console.ReadLine();

			if (!string.IsNullOrEmpty (username)) {
				// Check if username exists
				userKey = new Key ("test", "users", username);
				userRecord = client.Get (null, userKey);
				if (userRecord != null) {
					// Get new password
					string password;
					Console.WriteLine ("Enter new password for " + username + ":");
					password = Console.ReadLine ();

					WritePolicy writePolicy = new WritePolicy ();
					// record generation
					writePolicy.generation = userRecord.generation;
					writePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
					// password Bin
					passwordBin = new Bin ("password", password);
					client.Put (writePolicy, userKey, passwordBin);

					Console.WriteLine ("\nINFO: The password has been set to: " + password);
				} else {
					Console.WriteLine ("ERROR: User record not found!");
				}
			} else {
				Console.WriteLine ("ERROR: User record not found!");
			}
        } //updatePasswordUsingCAS

        public void batchGetUserTweets()
        {
            Record userRecord = null;
            Key userKey = null;

            // Get username
            string username;
            Console.WriteLine("\nEnter username:");
            username = Console.ReadLine();

            if (username != null && username.Length > 0)
            {
                // Check if username exists
                userKey = new Key("test", "users", username);
                userRecord = client.Get(null, userKey);
                if (userRecord != null)
                {
                    // Get how many tweets the user has
                    int tweetCount = int.Parse(userRecord.GetValue("tweetcount").ToString());

                    // Create an array of keys so we can initiate batch read operation
                    Key[] keys = new Key[tweetCount];
                    for (int i = 0; i < keys.Length; i++)
                    {
                        keys[i] = new Key("test", "tweets", (username + ":" + (i + 1)));
                    }

                    Console.WriteLine("\nHere's " + username + "'s tweet(s):\n");

                    // Initiate batch read operation
                    Record[] records = client.Get(null, keys);
                    for (int j = 0; j < records.Length; j++)
                    {
                        Console.WriteLine(records[j].GetValue("tweet"));
                    }
                }
                else
                {
                    Console.WriteLine("ERROR: User record not found!");
                }
            }
            else
            {
                Console.WriteLine("ERROR: User record not found!");
            }
        } //getUserTweets

        public void aggregateUsersByTweetCountByRegion()
        {
            ResultSet rs = null;
            try
            {
                int min;
                int max;
                Console.WriteLine("\nEnter Min Tweet Count:");
                min = int.Parse(Console.ReadLine());
                Console.WriteLine("Enter Max Tweet Count:");
                max = int.Parse(Console.ReadLine());

                Console.WriteLine("\nAggregating users with " + min + "-" + max + " tweets by region. Hang on...\n");

                // NOTE: UDF registration has been included here for convenience and to demonstrate the syntax. The recommended way of registering UDFs in production env is via AQL
                string luaDirectory = @"..\..\udf";
                LuaConfig.PackagePath = luaDirectory + @"\?.lua";

                string filename = "aggregationByRegion.lua";
                string path = Path.Combine(luaDirectory, filename);

                RegisterTask rt = client.Register(null, path, filename, Language.LUA);
                rt.Wait();

                string[] bins = { "tweetcount", "region" };
                Statement stmt = new Statement();
                stmt.SetNamespace("test");
                stmt.SetSetName("users");
                stmt.SetIndexName("tweetcount_index");
                stmt.SetBinNames(bins);
                stmt.SetFilters(Filter.Range("tweetcount", min, max));

                rs = client.QueryAggregate(null, stmt, "aggregationByRegion", "sum");

                if (rs.Next())
                {
                    Dictionary<object, object> result = (Dictionary<object, object>)rs.Object;
                    Console.WriteLine("Total Users in North: " + result["n"]);
                    Console.WriteLine("Total Users in South: " + result["s"]);
                    Console.WriteLine("Total Users in East: " + result["e"]);
                    Console.WriteLine("Total Users in West: " + result["w"]);
                }
            }
            finally
            {
                if (rs != null)
                {
                    // Close record set
                    rs.Close();
                }
            }
        } //aggregateUsersByTweetCountByRegion

        public void createUsers()
        {
            string[] genders = { "m", "f" };
            string[] regions = { "n", "s", "e", "w" };
            string[] randomInterests = {"Music","Football", "Soccer", "Baseball", "Basketball", "Hockey", "Weekend Warrior", "Hiking", "Camping", "Travel", "Photography"};
            string randomInterest;
            string username;
            List<object> userInterests = null;
            int totalInterests = 0;
            int start = 0;
            int end = 100000;
            int totalUsers = end - start;
            Random rnd1 = new Random();
            Random rnd2 = new Random();
            Random rnd3 = new Random(); 

            WritePolicy wPolicy = new WritePolicy();
            wPolicy.recordExistsAction = RecordExistsAction.UPDATE;

            Console.WriteLine("\nCreate " + totalUsers + " users. Press any key to continue...");
            Console.ReadLine();

            for (int j = start; j <= end; j++)
            {
                // Write user record
                username = "user" + j;
                Key key = new Key("test", "users", username);
                Bin bin1 = new Bin("username", "user" + j);
                Bin bin2 = new Bin("password", "pwd" + j);
                Bin bin3 = new Bin("gender", genders[rnd1.Next(0, 2)]);
                Bin bin4 = new Bin("region", regions[rnd2.Next(0, 4)]);
                Bin bin5 = new Bin("lasttweeted", 0);
                Bin bin6 = new Bin("tweetcount", 0);

                totalInterests = rnd3.Next(1, 7);
                userInterests = new List<object>();
                for (int t = 0; t < totalInterests; t++)
                {
                    randomInterest = randomInterests[rnd3.Next(0, 9)];
                    userInterests.Add(randomInterest);
                }
                Bin bin7 = Bin.AsList("interests", userInterests);

                client.Put(wPolicy, key, bin1, bin2, bin3, bin4, bin5, bin6, bin7);
                Console.WriteLine("Wrote user record for " + username);
            }

            Console.WriteLine("\nDone creating " + totalUsers + "!");
        } //createUsers
    }
}
