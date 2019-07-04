/*******************************************************************************
 * Copyright 2008-2016 by Aerospike.
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

// Original author: Tak Nakadai
// email: nakadai@aerospike.com

//==========================================================
// Includes
//

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>


#include <aerospike/aerospike.h>
#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/aerospike_scan.h>
#include <aerospike/aerospike_udf.h>
#include <aerospike/as_bin.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_error.h>
#include <aerospike/as_config.h>
#include <aerospike/as_key.h>
#include <aerospike/as_query.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_password.h>
#include <aerospike/as_record.h>
#include <aerospike/as_record_iterator.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_val.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_batch.h>
#include <aerospike/as_scan.h>

#define LOG(_fmt, _args...) { printf(_fmt "\n", ## _args);fflush(stdout); }

void updateUser(aerospike *as,as_key *key,as_policy_write *wpol, int tc,long ts);
void updateUserUsingOperate(aerospike *as,as_key *key, long ts);
bool display_tweets_cb(const as_val* p_val, void* udata);
bool queryBytweetcount_cb(const as_val* p_val, void* udata);
void queryTweetsByUsername(aerospike *as);
void queryUsersByTweetCount(aerospike *as);

void
createTweet(aerospike *as,as_policy_write *wpol){
	char username[256]={0};
	char tweet[256]={0};
	char tweetkey[512];
	long ts;

	as_error err;

	printf("\n********** Create Tweet **********\n");
	printf("Enter username: ");
	scanf("%s", username);

	if ( strlen(username) > 0 ){

		//Exercise K2
		as_key key;
		as_key_init(&key, "test", "users", username);
		as_record *urec = NULL;
		if (aerospike_key_get(as, &err, NULL, &key, &urec) ==
				AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			LOG("aerospike_key_get() returned %d - %s, expected "
					"AEROSPIKE_ERR_RECORD_NOT_FOUND", err.code, err.message);
			return;
		}

		int nextTweetCount= as_record_get_int64(urec, "tweetcount",INT64_MAX);
		nextTweetCount++;

		printf("Enter tweet for %s:",username);
		getchar(); //flush out carriage return
		fgets(tweet, sizeof(tweet), stdin);


		time_t t = time(NULL);
		ts = (long)t*1000;

		//Exercise K2
		sprintf(tweetkey,"%s:%d",username,nextTweetCount);
		as_key_init(&key, "test", "tweets", tweetkey);

		as_record rec;
		as_record_inita(&rec, 3);
		as_record_set_str(&rec, "tweet", tweet);
		as_record_set_int64(&rec, "ts", ts);
		as_record_set_str(&rec, "username", username);

		//                as_policy_write wpol;
		//                as_policy_write_init(&wpol);
		//                wpol.exists = AS_POLICY_EXISTS_IGNORE;

		if(aerospike_key_put(as, &err, wpol, &key, &rec) != AEROSPIKE_OK ){
			LOG("aerospike_key_put() returned %d - %s, expected "
					"NOT AEROSPIKE_OK", err.code, err.message);
			return;
		}
		as_key_init(&key, "test", "users", username);
		updateUser(as, &key, wpol, nextTweetCount, ts);
		printf("\nINFO: Tweet record created!\n");
	}
	else{
		printf("Invalid username\n");
	}


	return;
}

void
updateUser(aerospike *as,as_key *key,as_policy_write *wpol,int tc,long ts){

	//Exercise K2
	as_error err;
	as_record rec;
	as_record_inita(&rec, 2);
	as_record_set_int64(&rec, "lasttweeted", ts);
	as_record_set_int64(&rec, "tweetcount", tc);
	if(aerospike_key_put(as, &err, wpol, key, &rec) != AEROSPIKE_OK ){
		LOG("aerospike_key_put() returned %d - %s, expected "
				"NOT AEROSPIKE_OK", err.code, err.message);
		return;
	}
	printf("\nINFO: User record updated!\n");

	//Exercise K6 - Comment code above and uncomment code below
	//updateUserUsingOperate(as, key, ts);
}

void
updateUserUsingOperate(aerospike *as,as_key *key,long ts){
	char *username;
	char *password;
	char *gender;
	char *region;
	int tweetcount;
	long int lasttweeted;
	as_error err;

	//Exercise K6
	as_operations ops;
	as_operations_inita(&ops, 8);
	as_operations_add_incr(&ops, "tweetcount", 1);
	as_operations_add_write_int64(&ops, "lasttweeted", ts);
	as_operations_add_read(&ops, "username");
	as_operations_add_read(&ops, "password");
	as_operations_add_read(&ops, "gender");
	as_operations_add_read(&ops, "region");
	as_operations_add_read(&ops, "tweetcount");
	as_operations_add_read(&ops, "lasttweeted");

	as_record * rec = NULL;

	if (aerospike_key_operate(as, &err, NULL, key, &ops, &rec) != AEROSPIKE_OK) {
		printf("error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
	}

	username = as_record_get_str(rec,"username");
	password = as_record_get_str(rec,"password");
	gender = as_record_get_str(rec,"gender");
	region = as_record_get_str(rec,"region");
	tweetcount = as_record_get_int64(rec, "tweetcount",INT64_MAX);
	lasttweeted = as_record_get_int64(rec, "lasttweeted",INT64_MAX);
	printf("username:       %s\n",username);
	printf("password:       %s\n",password);
	printf("gender:         %s\n",gender);
	printf("region:         %s\n",region);
	printf("tweetcount:     %d\n",tweetcount);
	printf("lasttweeted:     %ld\n",lasttweeted);

	printf("\nINFO: User record updated using OPERATE.\n");
}

void
scanAllTweetsForAllUsers(aerospike *as){
	as_error err;

    //Exercise K4
	as_scan scan;
	as_scan_init(&scan, "test", "tweets");

	if (aerospike_scan_foreach(as, &err, NULL, &scan, display_tweets_cb, NULL) !=
			AEROSPIKE_OK) {
		LOG("aerospike_scan_foreach() returned %d - %s", err.code, err.message);
		as_scan_destroy(&scan);
		exit(-1);
	}
	as_scan_destroy(&scan);

}

bool
display_tweets_cb(const as_val* p_val, void* udata) {
	char * tweet;

	if (! p_val) {
		printf("Record set processing is complete.\n");
		return true;
	}
	as_record* p_rec = as_record_fromval(p_val);

	if (! p_rec) {
		printf("Record set returned non-as_record object.\n");
		return true;
	}

	tweet = as_record_get_str(p_rec,"tweet");
	printf("tweet:%s:\n",tweet);

	return true;

}

void
queryTweets(aerospike *as){
	queryTweetsByUsername(as);
	queryUsersByTweetCount(as);
}

void
queryTweetsByUsername(aerospike *as){
	printf("\n********** Query Tweets By Username **********\n");
	char username[256];
	as_error err;


	printf("Enter username: ");
	scanf("%s",username);

	if ( strlen(username) > 0 ){

		//Exercise Q3
		as_query query;
		as_query_init(&query, "test", "tweets");
		as_query_where_inita(&query, 1);
		as_query_where(&query, "username", as_string_equals(username));


		if (aerospike_query_foreach(as, &err, NULL, &query, display_tweets_cb, NULL) !=
				AEROSPIKE_OK) {
			LOG("aerospike_query_foreach() returned %d - %s\n", err.code, err.message);
			as_query_destroy(&query);
			exit(-1);
		}
		as_query_destroy(&query);
	}
}

void
queryUsersByTweetCount(aerospike *as){
	int min;
	int max;

	as_error err;

	printf("\n********** Query Users By Tweet Count Range **********\n");

	printf("\nEnter Min Tweet Count:");
	scanf("%d",&min);

	printf("\nEnter Max Tweet Count:");
	scanf("%d",&max);


	if ( max >= min ){

		//Exercise Q4
		as_query query;
		as_query_init(&query, "test", "users");
		as_query_where_inita(&query, 1);
		as_query_where(&query, "tweetcount", as_integer_range(min,max));

		if (aerospike_query_foreach(as, &err, NULL, &query, queryBytweetcount_cb, NULL) !=
				AEROSPIKE_OK) {
			LOG("aerospike_query_foreach() returned %d - %s\n", err.code, err.message);
			as_query_destroy(&query);
			exit(-1);
		}
		as_query_destroy(&query);
	}
}

bool
queryBytweetcount_cb(const as_val* p_val, void* udata) {
	char * username;
	int tweetcount;

	if (! p_val) {
		printf("SI query processing is complete.\n");
		return true;
	}
	as_record* p_rec = as_record_fromval(p_val);

	if (! p_rec) {
		printf("SI query callback returned non-as_record object.\n");
		return true;
	}

	username = as_record_get_str(p_rec,"username");
	tweetcount = as_record_get_int64(p_rec,"tweetcount",INT64_MAX);
	printf("username:%s tweetcount:%d:\n",username,tweetcount);

	return true;

}

