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

namespace AerospikeTraining
{
    class Program
    {

        static void Main(string[] args)
        {
            Console.WriteLine("***** Welcome to Aerospike Developer Training *****\n");
            AerospikeClient client = null;
            try
            {
                Console.WriteLine("INFO: Connecting to Aerospike cluster...");

                // Connecting to Aerospike cluster

                // Specify IP of one of the nodes in the cluster
                string asServerIP = "172.16.159.202";
                // Specity Port that the node is listening on
                int asServerPort = 3000;
                // Establish connection
                client = new AerospikeClient(asServerIP, asServerPort);

                // Check to see if the cluster connection succeeded
                if (client.Connected)
                {
                    Console.WriteLine("INFO: Connection to Aerospike cluster succeeded!\n");

                    // Create instance of UserService
                    UserService us = new UserService(client);
                    // Create instance of TweetService
                    TweetService ts = new TweetService(client);

                    // Present options
                    Console.WriteLine("What would you like to do:");
                    Console.WriteLine("1> Create A User And A Tweet");
                    Console.WriteLine("2> Read A User Record");
                    Console.WriteLine("3> Batch Read Tweets For A User");
                    Console.WriteLine("4> Scan All Tweets For All Users");
                    Console.WriteLine("5> Record UDF -- Update User Password");
                    Console.WriteLine("6> Query Tweets By Username And Users By Tweet Count Range");
                    Console.WriteLine("7> Stream UDF -- Aggregation Based on Tweet Count By Region");
                    Console.WriteLine("0> Exit");
                    Console.Write("\nSelect 0-7 and hit enter:");
                    byte feature = byte.Parse(Console.ReadLine());

                    if (feature != 0)
                    {
                        switch (feature)
                        {
                            case 1:
                                Console.WriteLine("\n********** Your Selection: Create User And A Tweet **********\n");
                                us.createUser();
                                ts.createTweet();
                                break;
                            case 2:
                                Console.WriteLine("\n********** Your Selection: Read A User Record **********\n");
                                us.getUser();
                                break;
                            case 3:
                                Console.WriteLine("\n********** Your Selection: Batch Read Tweets For A User **********\n");
                                us.batchGetUserTweets();
                                break;
                            case 4:
                                Console.WriteLine("\n**********  Your Selection: Scan All Tweets For All Users **********\n");
                                ts.scanAllTweetsForAllUsers();
                                break;
                            case 5:
                                Console.WriteLine("\n********** Your Selection: Record UDF -- Update User Password **********\n");
                                us.updatePasswordUsingUDF();
                                break;
                            case 6:
                                Console.WriteLine("\n**********  Your Selection: Query Tweets By Username And Users By Tweet Count Range **********\n");
                                ts.queryTweetsByUsername();
                                ts.queryUsersByTweetCount();
                                break;
                            case 7:
                                Console.WriteLine("\n**********  Your Selection: Stream UDF -- Aggregation Based on Tweet Count By Region **********\n");
                                us.aggregateUsersByTweetCountByRegion();
                                break;
                            case 12:
                                Console.WriteLine("\n********** Create Users **********\n");
                                us.createUsers();
                                break;
                            case 23:
                                Console.WriteLine("\n********** Create Tweets **********\n");
                                ts.createTweets();
                                break;
                            default:
                                Console.WriteLine("\n********** Invalid Selection **********\n");
                                break;
                        }
                    }
                }
                else
                {
                    Console.Write("ERROR: Connection to Aerospike cluster failed! Please check IP & Port settings and try again!");
                    Console.ReadLine();
                }
            }
            catch (AerospikeException e)
            {
                Console.WriteLine("AerospikeException - Message: " + e.Message);
                Console.WriteLine("AerospikeException - StackTrace: " + e.StackTrace);
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception - Message: " + e.Message);
                Console.WriteLine("Exception - StackTrace: " + e.StackTrace);
            }
            finally
            {
                if (client != null && client.Connected)
                {
                    // Close Aerospike server connection
                    client.Close();
                }
                Console.Write("\n\nINFO: Press any key to exit...");
                Console.ReadLine();
            }
            
        } //main

    }
}
