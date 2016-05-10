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

            string username;
            string password;
            string role;

            // Get username
            Console.WriteLine("Enter username: ");
            username = Console.ReadLine();

            if (username != null && username.Length > 0)
            {
                // Get password
                Console.WriteLine("Enter password for " + username + ":");
                password = Console.ReadLine();

                // Get role
                Console.WriteLine("Enter a role for " + username + ":");
                role = Console.ReadLine();

                string[] roles = { role };
                //TODO: Create User
                //Exercise 2

                Console.WriteLine("\nINFO: User created!");
            }
        }

        public void getUser()
        {
            // Get username
            string username;
            Console.WriteLine("\nEnter username:");
            username = Console.ReadLine();

            if (!string.IsNullOrEmpty(username))
            {

                //TODO: Read User
                //Exercise 3
                User user = null;
                if (user != null)
                {
                    Console.WriteLine("\nINFO: User read successfully! Here are the details:\n");
                    Console.WriteLine("roles:     " + string.Join(", ", user.roles));
                }
                else
                {
                    Console.WriteLine("ERROR: User not found!");
                }
            }
            else
            {
                Console.WriteLine("ERROR: User not found!");
            }
        }


        public void dropUser()
        {
            // Get username
            string username;
            Console.WriteLine("\nEnter username:");
            username = Console.ReadLine();

            if (!string.IsNullOrEmpty(username))
            {
                //TODO: Drop User
                //Exercise 4
                Console.WriteLine("\nINFO: User dropped\n");
            }
            else
            {
                Console.WriteLine("ERROR: User not found!");
            }
        }


        public void grantRole()
        {
            Console.WriteLine("\n********** Grant Role **********\n");

            string username;
            string role;

            // Get username
            Console.WriteLine("Enter username: ");
            username = Console.ReadLine();

            if (username != null && username.Length > 0)
            {
                // Get role
                Console.WriteLine("Enter a role for " + username + ":");
                role = Console.ReadLine();

                string[] roles = { role };
                //TODO: Grant Role
                //Exercise 5

                Console.WriteLine("\nINFO: Role granted!");
            }
        }


        public void revokeRole()
        {
            Console.WriteLine("\n********** Revoke Role **********\n");

            string username;
            string role;

            // Get username
            Console.WriteLine("Enter username: ");
            username = Console.ReadLine();

            if (username != null && username.Length > 0)
            {
                // Get role
                Console.WriteLine("Enter a role to revoke from " + username + ":");
                role = Console.ReadLine();

                string[] roles = { role };
                //TODO: Revoke Role
                //Exercise 6

                Console.WriteLine("\nINFO: Role revoked!");
            }
        }
    }
}
