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
                string asServerIP = "192.168.1.151";
                // Specity Port that the node is listening on
                int asServerPort = 3000;
                // Establish connection
                ClientPolicy clientPolicy = new ClientPolicy();
                clientPolicy.user = "superman";
                clientPolicy.password = "krypton";
                client = new AerospikeClient(clientPolicy, asServerIP, asServerPort);

                // Check to see if the cluster connection succeeded
                if (client.Connected)
                {
                    Console.WriteLine("INFO: Connection to Aerospike cluster succeeded!\n");

                    // Create instance of UserService
                    UserService us = new UserService(client);
                    // Create instance of RoleService
                    RoleService rs = new RoleService(client);

                    // Present options
                    
                    Console.WriteLine("\nWhat would you like to do:\n");
                    Console.WriteLine("1> Create User\n");
                    Console.WriteLine("2> Read User\n");
                    Console.WriteLine("3> Grant Role to User\n");
                    Console.WriteLine("4> Revoke Role from User\n");
                    Console.WriteLine("5> Drop User\n");
                    Console.WriteLine("6> Create Role\n");
                    Console.WriteLine("7> Read Role\n");
                    Console.WriteLine("8> Grant Privilege to Role\n");
                    Console.WriteLine("9> Revoke Privilege from Role\n");
                    Console.WriteLine("10> Drop Role\n");
                    Console.WriteLine("0> Exit\n");
                    Console.Write("\nSelect 0-10 and hit enter:\n");

                    int feature = int.Parse(Console.ReadLine());

                    if (feature != 0)
                    {
                        switch (feature)
                        {
                            case 1:
                                Console.WriteLine("\n********** Your Selection: Create User **********\n");
                                us.createUser();
                                break;
                            case 2:
                                Console.WriteLine("\n********** Your Selection: Read User **********\n");
                                us.getUser();
                                break;
                            case 3:
                                Console.WriteLine("\n********** Your Selection: Grant Role **********\n");
                                us.grantRole();
                                break;
                            case 4:
                                Console.WriteLine("\n**********  Your Selection: Revoke Role **********\n");
                                us.revokeRole();
                                break;
                            case 5:
                                Console.WriteLine("\n********** Your Selection: Drop User **********\n");
                                us.dropUser();
                                break;
                            case 6:
                                Console.WriteLine("\n**********  Your Selection: Create Role **********\n");
                                rs.createRole();
                                break;
                            case 7:
                                Console.WriteLine("\n**********  Your Selection: Read Role **********\n");
                                rs.readRole();
                                break;
                            case 8:
                                Console.WriteLine("\n********** Your Selection: Grant Privilege **********\n");
                                rs.grantPrivilege();
                                break;
                            case 9:
                                Console.WriteLine("\n********** Your Selection: Revoke Privilege **********\n");
                                rs.revokePrivilege();
                                break;
                            case 10:
                                Console.WriteLine("\n********** Your Selection: Drop Role **********\n");
                                rs.dropRole();
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
