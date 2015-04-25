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
using System.IO;
using Aerospike.Client;

namespace AerospikeTraining
{
    class UtilityService
    {
        private AerospikeClient client;

        public UtilityService(AerospikeClient c)
        {
            this.client = c;
        }

        public void createSecondaryIndexes()
        {
            // NOTE: Index creation has been included in here for convenience and to demonstrate the syntax. The recommended way of creating indexes in production env is via AQL

            IndexTask task1 = client.CreateIndex(null, "test", "tweets", "username_index", "username", IndexType.STRING);
            task1.Wait();
            Console.WriteLine("Done creating secondary index on: set=tweets, bin=username");

            Console.WriteLine("\nCreating secondary index on: set=tweets, bin=ts. Hang on...");
            IndexTask task2 = client.CreateIndex(null, "test", "tweets", "ts_index", "ts", IndexType.NUMERIC);
            task2.Wait();
            Console.WriteLine("Done creating secondary index on: set=tweets, bin=ts");

            Console.WriteLine("\nCreating secondary index on: set=users, bin=tweetcount. Hang on...");
            IndexTask task3 = client.CreateIndex(null, "test", "users", "tweetcount_index", "tweetcount", IndexType.NUMERIC);
            task3.Wait();
            Console.WriteLine("Done creating secondary index on: set=users, bin=tweetcount");
        }

        /// <summary>
        /// Example functions not in use
        /// </summary>
        private void deleteTweets()
        {
            RecordSet rs = null;
            try
            {
                // Get username
                string username;
                Console.WriteLine("\nEnter username:");
                username = Console.ReadLine();

				if (username != null && username.Length > 0)
                {
                    // Check if username exists
                    Key userKey = new Key("test", "users", username);
                    Record userRecord = client.Get(null, userKey);
                    if (userRecord != null)
                    {
                        WritePolicy wPolicy = new WritePolicy();
                        wPolicy.recordExistsAction = RecordExistsAction.UPDATE;

                        string[] bins = { "tweet" };
                        Statement stmt = new Statement();
                        stmt.SetNamespace("test");
                        stmt.SetSetName("tweets");
                        stmt.SetIndexName("username_index");
                        stmt.SetBinNames(bins);
                        stmt.SetFilters(Filter.Equal("username", username));

                        Console.WriteLine("\nDeleting " + username + "'s tweet(s):\n");

                        rs = client.Query(null, stmt);
                        while (rs.Next())
                        {
                            Record r = rs.Record;
                            Console.WriteLine(r.GetValue("tweet"));
                            client.Delete(null, rs.Key);
                        }
                        //Update tweetcount and timestamp to reflect this
                        client.Operate(wPolicy, userKey, Operation.Put(new Bin("tweetcount", 0)), Operation.Put(new Bin("lasttweeted", 0)));
                        rs.Close();
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
            }
            finally
            {
                if (rs != null)
                {
                    // Close record set
                    rs.Close();
                }
            }
        }

        private void add()
        {
            // C# Add Example
            Key userKey = new Key("test", "users", "user1234");
            Bin bin1 = new Bin("count", 2);
            Bin bin2 = new Bin("count", 3);
            client.Add(null, userKey, bin2);
        }

        private void append()
        {
            // C# Append Example
            Key userKey = new Key("test", "users", "user1234");
            Bin bin1 = new Bin("greet", "hello");
            Bin bin2 = new Bin("greet", " world");
            client.Append(null, userKey, bin2);
        }

        private void exists()
        {
            // C# Exists Example
            Key userKey = new Key("test", "users", "user1234");
            bool recordKeyExists = client.Exists(null, userKey);
        }

        private void touch()
        {
            // C# Touch Example
            Key userKey = new Key("test", "users", "user1234");
            client.Touch(null, userKey);
        }

    }
}
