#!/usr/bin/env python
# 
#  * Copyright 2012-2014 by Aerospike.
#  *
#  * Permission is hereby granted, free of charge, to any person obtaining a copy
#  * of this software and associated documentation files (the "Software"), to
#  * deal in the Software without restriction, including without limitation the
#  * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
#  * sell copies of the Software, and to permit persons to whom the Software is
#  * furnished to do so, subject to the following conditions:
#  *
#  * The above copyright notice and this permission notice shall be included in
#  * all copies or substantial portions of the Software.
#  *
#  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
#  * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
#  * IN THE SOFTWARE.
#  
from __future__ import print_function
import aerospike
import sys
import time
import json
from aerospike import predicates as p

class RoleService(object):
    #client 

    def __init__(self, client):
        self.client = client

    def create_role(self):
        print("\n********** Create User **********\n")

        role = str()
        privilege_code = str()
        code = int()

        #  Get role
        role = raw_input("Enter role: ")
        if len(role) > 0:
            #  Get privilege_code
            privilege_code = input("Enter a privilege code\n(0 = read, 1 = read-write, 2 = read-write-udf, 3 = data-admin, 4 = sys-admin, 5 = user-admin):")
            if privilege_code == 0:
                code = aerospike.PRIV_READ
            elif privilege_code == 1:
                code = aerospike.PRIV_READ_WRITE
            elif privilege_code == 2:
                code = aerospike.PRIV_READ_WRITE_UDF
            elif privilege_code == 3:
                code = aerospike.PRIV_DATA_ADMIN
            elif privilege_code == 4:
                code = aerospike.PRIV_SYS_ADMIN
            elif privilege_code == 5:
                code = aerospike.PRIV_USER_ADMIN

            privileges = [{'code': code }]
            self.client.admin_create_role(role, privileges)

            print("\nINFO: Role created!")

    def read_role(self):

        #  Get role
        role = str()
        role = raw_input("Enter role: ")

        if len(role) > 0:
            #  Check if role exists
            print(self.client.admin_query_role(role))

        else:
            print("ERROR: Role not found!\n")

    def drop_role(self):

        #  Get role
        role = str()
        role = raw_input("Enter role: ")

        if len(role) > 0:
            self.client.admin_drop_role(role)
            print("\nINFO: Dropped role!")
        else:
            print("ERROR: Role not found!\n")

    def grant_privilege(self):
        print("\n********** Revoke Privilege **********\n")

        role = str()
        privilege_code = str()
        code = int()

        role = raw_input("Enter role: ")
        if len(role) > 0:
            #  Get privilege_code
            privilege_code = input("Enter a privilege code\n(0 = read, 1 = read-write, 2 = read-write-udf, 3 = data-admin, 4 = sys-admin, 5 = user-admin):")
            if privilege_code == 0:
                code = aerospike.PRIV_READ
            elif privilege_code == 1:
                code = aerospike.PRIV_READ_WRITE
            elif privilege_code == 2:
                code = aerospike.PRIV_READ_WRITE_UDF
            elif privilege_code == 3:
                code = aerospike.PRIV_DATA_ADMIN
            elif privilege_code == 4:
                code = aerospike.PRIV_SYS_ADMIN
            elif privilege_code == 5:
                code = aerospike.PRIV_USER_ADMIN

        privileges = [{'code': code}]
        self.client.admin_grant_privileges(role, privileges)

        print("\nINFO: Privilege granted!")

    def revoke_privilege(self):
        print("\n********** Grant Privilege **********\n")

        role = str()
        privilege_code = str()
        code = int()

        role = raw_input("Enter role: ")
        if len(role) > 0:
            #  Get privilege_code
            privilege_code = input(
                "Enter a privilege code\n(0 = read, 1 = read-write, 2 = read-write-udf, 3 = data-admin, 4 = sys-admin, 5 = user-admin):")
            if privilege_code == 0:
                code = aerospike.PRIV_READ
            elif privilege_code == 1:
                code = aerospike.PRIV_READ_WRITE
            elif privilege_code == 2:
                code = aerospike.PRIV_READ_WRITE_UDF
            elif privilege_code == 3:
                code = aerospike.PRIV_DATA_ADMIN
            elif privilege_code == 4:
                code = aerospike.PRIV_SYS_ADMIN
            elif privilege_code == 5:
                code = aerospike.PRIV_USER_ADMIN

        privileges = [{'code': code}]
        self.client.admin_revoke_privileges(role, privileges)

        print("\nINFO: Privilege revoked!")