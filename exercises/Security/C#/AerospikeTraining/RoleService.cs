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
    class RoleService
    {
        private AerospikeClient client;

        public RoleService(AerospikeClient c)
        {
            this.client = c;
        }

        public void createRole()
        {
            Console.WriteLine("\n********** Create Role **********\n");

            int privilegeCode;
            string role;

            // Get username
            Console.WriteLine("Enter role: ");
            role = Console.ReadLine();

            if (role != null && role.Length > 0)
            {

                // Add privilege
                Console.WriteLine("Enter a privilege code\n(0 = read, 1 = read-write, 2 = read-write-udf, 4 = sys-admin, 5 = user-admin):");
                privilegeCode = int.Parse(Console.ReadLine());
                Privilege privilege = new Privilege();
                switch (privilegeCode)
                {
                    case 0: privilege.code = PrivilegeCode.READ; break;
                    case 1: privilege.code = PrivilegeCode.READ_WRITE; break;
                    case 2: privilege.code = PrivilegeCode.READ_WRITE_UDF; break;
                    //MISSING FROM API! case 3: privilege.code = PrivilegeCode.DATA_ADMIN; break; 
                    case 4: privilege.code = PrivilegeCode.SYS_ADMIN; break;
                    case 5: privilege.code = PrivilegeCode.USER_ADMIN; break;
                }

                // Create role
                AdminPolicy adminPolicy = new AdminPolicy();
                Privilege[] privileges = { privilege };
                //TODO: Create Role
                //Exercise 7

                Console.WriteLine("\nINFO: Role created!");
            }
        }




        public void readRole()
        {
            // Get role
            Console.WriteLine("\nEnter role:");
            String roleName = Console.ReadLine();

		    if (roleName != null && roleName.Length > 0) {

                AdminPolicy adminPolicy = new AdminPolicy();
                //TODO: Read Role
                //Exercise 8
                Role role = null;
                if (role != null)
                {
                    Console.WriteLine("\nINFO: Role read successfully! Here are the details:\n");
                    Console.WriteLine(string.Join(", ", role.privileges));
                }
                else
                {
                    Console.WriteLine("ERROR: Role not found!\n");
                }
            } else {
                Console.WriteLine("ERROR: Role not found!\n");
            }
        }

        public void dropRole()
        {
            // Get role
            Console.WriteLine("\nEnter role:");
            String roleName = Console.ReadLine();

		    if (roleName != null && roleName.Length > 0) {
                AdminPolicy adminPolicy = new AdminPolicy();
                //TODO: Drop Role
                //Exercise 9
                Console.WriteLine("\nINFO: The role has been dropped.");
            } else {
                Console.WriteLine("ERROR: Role not found!");
            }
        }

        public void grantPrivilege()
        {

            // Get role
            Console.WriteLine("\nEnter role:");
            String roleName = Console.ReadLine();
            int privilegeCode;

		    if (roleName != null && roleName.Length > 0) {
                // Get new privilege
                Console.WriteLine("Enter a privilege code\n(0 = read, 1 = read-write, 2 = read-write-udf, 4 = sys-admin, 5 = user-admin):");
                privilegeCode = int.Parse(Console.ReadLine());
                Privilege privilege = new Privilege();
                switch (privilegeCode)
                {
                    case 0: privilege.code = PrivilegeCode.READ; break;
                    case 1: privilege.code = PrivilegeCode.READ_WRITE; break;
                    case 2: privilege.code = PrivilegeCode.READ_WRITE_UDF; break;
                    //MISSING FROM API! case 3: privilege.code = PrivilegeCode.DATA_ADMIN; break; 
                    case 4: privilege.code = PrivilegeCode.SYS_ADMIN; break;
                    case 5: privilege.code = PrivilegeCode.USER_ADMIN; break;
                }

                AdminPolicy adminPolicy = new AdminPolicy();
                Privilege[] privileges = { privilege };

                //TODO: Grant Privilege
                //Exercise 10
                Console.WriteLine("\nINFO: The privilege has been added to: " + roleName);
            } else {
                Console.WriteLine("ERROR: Role not found!");
            }
        }

        public void revokePrivilege()
        {

            // Get role
            Console.WriteLine("\nEnter role:");
            String roleName = Console.ReadLine();

		    if (roleName != null && roleName.Length > 0) {
                // Get new privilege
                Console.WriteLine("Enter a privilege code\n(0 = read, 1 = read-write, 2 = read-write-udf, 4 = sys-admin, 5 = user-admin):");
                int privilegeCode = int.Parse(Console.ReadLine());
                Privilege privilege = new Privilege();
                switch (privilegeCode)
                {
                    case 0: privilege.code = PrivilegeCode.READ; break;
                    case 1: privilege.code = PrivilegeCode.READ_WRITE; break;
                    case 2: privilege.code = PrivilegeCode.READ_WRITE_UDF; break;
                    //MISSING FROM API! case 3: privilege.code = PrivilegeCode.DATA_ADMIN; break; 
                    case 4: privilege.code = PrivilegeCode.SYS_ADMIN; break;
                    case 5: privilege.code = PrivilegeCode.USER_ADMIN; break;
                }

                // Create user
                AdminPolicy adminPolicy = new AdminPolicy();
                Privilege[] privileges = { privilege };

                //TODO: Revoke Privilege
                //Exercise 11
                Console.WriteLine("\nINFO: The privilege has been revoked from: " + roleName);
            } else {
                Console.WriteLine("ERROR: Role not found!");
            }
        }
    }
}
