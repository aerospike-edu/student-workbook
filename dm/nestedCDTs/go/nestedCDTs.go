/* 
 * Copyright 2012-2015 Aerospike, Inc.
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
	"flag"
	"fmt"
	. "github.com/aerospike/aerospike-client-go"
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
	fmt.Println("***** Nested CDTs Example *****\n")
	fmt.Println("INFO: Connecting to Aerospike cluster...")
	// Establish connection to Aerospike server
	client, err := NewClient("127.0.0.1", 3000) 
	panicOnError(err)
	defer client.Close()

	if !client.IsConnected() {
		fmt.Println("ERROR: Connection to Aerospike cluster failed! Please check the server settings and try again!")
		fmt.Scanf("%s", &c)

	} else {
		fmt.Println("INFO: Connection to Aerospike cluster succeeded!")
		fmt.Println("1: Delete old record, add record: test.s1 PK=1")
                AddRecord(client)
		ChangeL1K1ToMap(client)
                UpdateL1K1Map(client)
		ChangeL2K1ToMap(client)
		UpdateL2K1Map(client)
		UpdateL3K3Value(client)
		UpdateL3K3ValueToList(client)
		UpdateL3K3ListValues(client)
		num := GetL3K3MaxListValue(client)
                fmt.Println("Max list item value = ", num)
        }
}

func AddRecord(client *Client) {
	wPolicy := NewWritePolicy(0, 0) // generation = 0, expiration = 0
	wPolicy.RecordExistsAction = UPDATE
	key1, _ := NewKey("test", "s1", 1)
        client.Delete(wPolicy, key1)
	var m1 = map[string]int{"l1k1":11, "l1k2":12, "l1k3":13}
	var bin = NewBin("myMap", m1)
	client.PutBins(wPolicy, key1, bin)
}

func ChangeL1K1ToMap(client *Client) {
	wPolicy := NewWritePolicy(0, 0) // generation = 0, expiration = 0
	wPolicy.RecordExistsAction = UPDATE
	key1, _ := NewKey("test", "s1", 1)
	var m2 = map[string]int{"l2k1":0}
	mPolicy := NewMapPolicy(MapOrder.UNORDERED,MapWriteMode.UPDATE)
	client.Operate(wPolicy, key1, MapPutOp(mPolicy,"myMap", "l1k1",m2)) 
}
func UpdateL1K1Map(client *Client) {
	wPolicy := NewWritePolicy(0, 0) // generation = 0, expiration = 0
	wPolicy.RecordExistsAction = UPDATE
	key1, _ := NewKey("test", "s1", 1)
	var m2 = map[interface{}]interface{}{"l2k1":21, "l2k2":22, "l2k3":23}
        ctx1 :=  CtxMapKey(NewStringValue("l1k1"))
	mPolicy := NewMapPolicy(MapOrder.UNORDERED,MapWriteMode.UPDATE)
	client.Operate(wPolicy, key1, MapPutItemsOp(mPolicy,"myMap", m2, ctx1)) 
}
func ChangeL2K1ToMap(client *Client) {
	wPolicy := NewWritePolicy(0, 0) // generation = 0, expiration = 0
	wPolicy.RecordExistsAction = UPDATE
	key1, _ := NewKey("test", "s1", 1)
	var m2 = map[string]int{"l3k1":0}
        ctx1 :=  CtxMapKey(NewStringValue("l1k1"))
	mPolicy := NewMapPolicy(MapOrder.UNORDERED,MapWriteMode.UPDATE)
	client.Operate(wPolicy, key1, MapPutOp(mPolicy,"myMap", "l2k1",m2, ctx1)) 
}
func UpdateL2K1Map(client *Client) {
	wPolicy := NewWritePolicy(0, 0) // generation = 0, expiration = 0
	wPolicy.RecordExistsAction = UPDATE
	key1, _ := NewKey("test", "s1", 1)
	var m2 = map[interface{}]interface{}{"l3k1":31, "l3k2":32, "l3k3":33}
        ctx1 :=  CtxMapKey(NewStringValue("l1k1"))
        ctx2 :=  CtxMapKey(NewStringValue("l2k1"))
	mPolicy := NewMapPolicy(MapOrder.UNORDERED,MapWriteMode.UPDATE)
	client.Operate(wPolicy, key1, MapPutItemsOp(mPolicy,"myMap", m2, ctx1, ctx2)) 
}
func UpdateL3K3Value(client *Client) {
	wPolicy := NewWritePolicy(0, 0) // generation = 0, expiration = 0
	wPolicy.RecordExistsAction = UPDATE
	key1, _ := NewKey("test", "s1", 1)
        ctx1 :=  CtxMapKey(NewStringValue("l1k1"))
        ctx2 :=  CtxMapKey(NewStringValue("l2k1"))
	mPolicy := NewMapPolicy(MapOrder.UNORDERED,MapWriteMode.UPDATE)
        //Change from 33 to 99
	client.Operate(wPolicy, key1, MapPutOp(mPolicy,"myMap", "l3k3", 99, ctx1, ctx2)) 
}
func UpdateL3K3ValueToList(client *Client) {
	wPolicy := NewWritePolicy(0, 0) // generation = 0, expiration = 0
	wPolicy.RecordExistsAction = UPDATE
	key1, _ := NewKey("test", "s1", 1)
        ctx1 :=  CtxMapKey(NewStringValue("l1k1"))
        ctx2 :=  CtxMapKey(NewStringValue("l2k1"))
	list := []int{}
	mPolicy := NewMapPolicy(MapOrder.UNORDERED,MapWriteMode.UPDATE)
        //Change from 99 to an empty list
	client.Operate(wPolicy, key1, MapPutOp(mPolicy,"myMap", "l3k3", list, ctx1, ctx2)) 
}
func UpdateL3K3ListValues(client *Client) {
	wPolicy := NewWritePolicy(0, 0) // generation = 0, expiration = 0
	wPolicy.RecordExistsAction = UPDATE
	key1, _ := NewKey("test", "s1", 1)
        ctx1 :=  CtxMapKey(NewStringValue("l1k1"))
        ctx2 :=  CtxMapKey(NewStringValue("l2k1"))
        ctx3 :=  CtxMapKey(NewStringValue("l3k3"))
        ctx := []*CDTContext{ctx1, ctx2, ctx3}
	lPolicy := NewListPolicy(ListOrderOrdered,ListWriteFlagsAddUnique|ListWriteFlagsNoFail|ListWriteFlagsPartial)
        //Update List using List operation but first change list order from default unordered
	client.Operate(wPolicy, key1, 
          ListSetOrderOp("myMap", ListOrderOrdered, ctx1, ctx2, ctx3),
          ListAppendWithPolicyContextOp(lPolicy,"myMap", ctx, 0,4,1,4)) 
}
func GetL3K3MaxListValue(client *Client) int {
	wPolicy := NewWritePolicy(0, 0) // generation = 0, expiration = 0
	wPolicy.RecordExistsAction = UPDATE
	key1, _ := NewKey("test", "s1", 1)
        ctx1 :=  CtxMapKey(NewStringValue("l1k1"))
        ctx2 :=  CtxMapKey(NewStringValue("l2k1"))
        ctx3 :=  CtxMapKey(NewStringValue("l3k3"))
        // Get max list value (rank = -1)
	rec,_ := client.Operate(wPolicy, key1, 
          ListGetByRankOp("myMap", -1, ListReturnTypeValue, ctx1, ctx2, ctx3))
        return rec.Bins["myMap"].(int)
}
