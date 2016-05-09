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

class UserService(object):
    #client 

    def __init__(self, client):
        self.client = client

    def createUser(self):
        print("\n********** Create User **********\n")

        username = str()
        password = str()
        role = str()

        #  Get username
        username = raw_input("Enter username: ")
        if len(username) > 0:
            #  Get password
            password = raw_input("Enter password for " + username + ":")
            #  Get role
            role = raw_input("Enter a role for  " + username + ":")

            self.client.admin_create_user(username, password, [role])

            print("\nINFO: User created!")

    def getUser(self):

        #  Get username
        username = str()
        username = raw_input("Enter username: ")

        if len(username) > 0:
            #  Check if username exists
            print(self.client.admin_query_user(username))

        else:
            print("ERROR: User not found!\n")

    def dropUser(self):

        #  Get username
        username = str()
        username = raw_input("Enter username: ")

        if len(username) > 0:
            self.client.admin_drop_user(username)
            print("\nINFO: Dropped user!")
        else:
            print("ERROR: User not found!\n")

    def grantRole(self):
        print("\n********** Grant Role **********\n")

        username = str()
        role = str()

        #  Get username
        username = raw_input("Enter username: ")
        if len(username) > 0:
            #  Get role
            role = raw_input("Enter a role for  " + username + ":")

            self.client.admin_grant_roles(username, [role])

            print("\nINFO: Role granted!")

    def revokeRole(self):
        print("\n********** Revoke Role **********\n")

        username = str()
        role = str()

        #  Get username
        username = raw_input("Enter username: ")
        if len(username) > 0:
            #  Get role
            role = raw_input("Enter a role:")

            self.client.admin_revoke_roles(username, [role])

            print("\nINFO: Role revoked!")