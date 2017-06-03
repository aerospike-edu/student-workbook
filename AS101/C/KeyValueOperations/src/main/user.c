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

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>
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

#define LOG(_fmt, _args...) { printf(_fmt "\n", ## _args);fflush(stdout); }

bool batch_read_cb(const as_batch_read* results, uint32_t n, void* udata);
bool register_udf(aerospike* p_as, const char* udf_file_path);
bool streamUDF_cb(const as_val* p_val, void* udata);
int split( char *str, const char *delim, char *outlist[] );

void
createUser(aerospike *as,as_policy_write* wpol){
	char username[256];
	char password[256];
	char gender[256];
	char region[256];
	char interests[1256];
	char *interestslist[256];
	int i,cnt;

	as_error err;

	printf("Enter username: ");
	scanf("%s",username);

	if ( strlen(username) > 0 ){
		printf("Enter password for %s:",username);
		scanf("%s",password);

		printf("Select gender (f or m) for %s:",username);
		scanf("%s",gender);

		gender[1] = '\0';
		printf("Select region (north, south, east or west) for %s;",username);
		scanf("%s",region);

		region[1] = '\0';
		printf("Enter comma-separated interests for %s:",username);
		scanf("%s",interests);


		as_key key;
		as_key_init(&key, "test", "users", username);

		//Exercise K2
		as_record rec;
		/*//Add code ...
		as_record_inita(&rec, ...);
		as_record_set_str(&rec, "username", ...);
		as_record_set_str(&rec, ..., password);
		as_record_set_str(&rec, "gender", gender);
		as_record_set_str(&rec, "region", region);
		as_record_set_int64(&rec, ..., ...);
		as_record_set_int64(..., "tweetcount", 0);

		as_arraylist as_interestlist;
		cnt = split(...);
		as_arraylist_inita(...);
		for( i=0; i < cnt; i++ ){
			as_arraylist_append_str(...);
		}

		as_record_set_list(&rec, "interests", ...);

		if(aerospike_key_put(..., &rec) != AEROSPIKE_OK ){
			LOG("aerospike_key_put() returned %d - %s, expected "
					"NOT AEROSPIKE_OK", err.code, err.message);
			return;
		}
		*/
		printf("\nINFO: User record created!\n");
	}


	return;
}

void
getUser(aerospike *as){
	char username[256];
	char *password;
	char *gender;
	char *region;
	int tweetcount;
	as_list *interestlist;

	as_error err;

	printf("Enter username: ");
	scanf("%s",username);

	if ( strlen(username) > 0 ){

		//Exercise K2
		as_key key;
		/*//Add code ...
		as_key_init(&key, ...);
		as_record *urec = ...;  //If you don't initialize to NULL ==> segmentation fault!
		if (aerospike_key_get(as,...) ==
				AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			LOG("aerospike_key_get() returned %d - %s, expected "
					"AEROSPIKE_ERR_RECORD_NOT_FOUND", err.code, err.message);
			return;
		}
		password = as_record_get_str(urec,"password");
		gender = as_record_get_str(...);
		region = as_record_get_str(...);
		tweetcount = as_record_get_int64(...);
		interestlist = as_record_get_list(...);
		*/

		printf("username:	%s\n",username);
		printf("password:	%s\n",password);
		printf("gender:		%s\n",gender);
		printf("region:		%s\n",region);
		printf("tweetcount:	%d\n",tweetcount);
		printf("interests:	");
		uint32_t isize = as_list_size(interestlist);
		for(uint32_t i = 0; i < isize; i++){
			as_val *v = as_list_get(interestlist,i);
			char *s = as_val_tostring(v);
			printf("%s,",s);
		}
		printf("\n");


	}
}

void
batchGetUserTweets(aerospike *as){
	char username[256];
	char tweetkey[256];
	char *tweetkeyp;

	as_error err;

	printf("Enter username: ");
	scanf("%s",username);

	if ( strlen(username) > 0 ){
		//Exercise K3
		as_key key;
		as_key_init(&key, "test", "users", username);
		as_record *urec = NULL;
		if (aerospike_key_get(as, &err, NULL, &key, &urec) ==
				AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			LOG("aerospike_key_get() returned %d - %s, expected "
					"AEROSPIKE_ERR_RECORD_NOT_FOUND", err.code, err.message);
		}
		int tweetCount= as_record_get_int64(urec, "tweetcount",INT64_MAX);

		/*//Add code ...
		as_batch batch;
		as_batch_inita(...);

		for (uint32_t i = 0; i < tweetCount; i++) {
			sprintf(...);
			tweetkeyp = (char *)malloc(sizeof(tweetkey));
			strcpy(tweetkeyp,tweetkey);
			as_key_init_str(...);
		}
		// User can use aerospike_batch_read. version >=  3.6.0
		// Review batch_read_cb function
		if (aerospike_batch_get(..., batch_read_cb, NULL) !=
				AEROSPIKE_OK) {
			as_batch_destroy(&batch);
			LOG("aerospike_batch_get() returned %d - %s", err.code, err.message);
			exit(-1);
		}
		as_batch_destroy ... //Release the resources
		*/

	}

}

bool
batch_read_cb(const as_batch_read* results, uint32_t n, void* udata)
{
	char * tweet;

	LOG("batch read callback returned %u record results:", n);

	for (uint32_t i = 0; i < n; i++) {
		tweet = as_record_get_str((as_record *)&results[i].record,"tweet");
		printf("index %u, key %s: tweet :%s\n", i,
				as_string_getorelse((as_string*)results[i].key->valuep, "ERR"),tweet);
	}
	return true;
}

void
updatePasswordUsingCAS(aerospike* as,as_policy_write* wpol){
	char username[256];
	char password[256];

	as_error err;

	printf("Enter username: ");
	scanf("%s",username);

	if ( strlen(username) > 0 ){
		as_key key;
		as_key_init(&key, "test", "users", username);
		as_record *urec = NULL;
		if (aerospike_key_get(as, &err, NULL, &key, &urec) ==
				AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			LOG("aerospike_key_get() returned %d - %s, expected "
					"AEROSPIKE_ERR_RECORD_NOT_FOUND", err.code, err.message);
			return;
		}
		printf("Enter new password for %s:",username);
		scanf("%s",password);

		if ( strlen(password) > 0 ){

			//Exercise K5
			/* //Add code ...
			 *
			wpol->gen = ...

			//Note: We are passing back urec that we read with a modified bin and
			//its generation that we recently read.

			as_record_set_str(...);
			if(aerospike_key_put(...) != AEROSPIKE_OK ){
				LOG("aerospike_key_put() returned %d - %s, expected "
						"NOT AEROSPIKE_OK", err.code, err.message);
				printf("\n*** Error updating user password ***\n");
				return;
			}
			else {
				printf("\nINFO: User password changed!\n");
			}
			*/
		}
		else{
			printf("\nINFO: Invalid User password entered!\n");
		}
	}
}

void
updatePasswordUsingUDF(aerospike* as){
	char username[256];
	char password[256];

	as_error err;

	printf("Enter username: ");
	scanf("%s",username);

	if ( strlen(username) > 0 ){
		as_key key;
		as_key_init(&key, "test", "users", username);
		//Exercise R2
		as_record *urec = NULL;  //Must initialize to NULL

		if (aerospike_key_get(as, &err, NULL, &key, &urec) ==
				AEROSPIKE_ERR_RECORD_NOT_FOUND) {
			LOG("aerospike_key_get() returned %d - %s, expected "
					"AEROSPIKE_ERR_RECORD_NOT_FOUND", err.code, err.message);
			return;
		}
		printf("Enter new password for %s:",username);
		scanf("%s",password);

		if ( strlen(password) > 0 ){
			//Exercise R2
			//UDF registration via C Client requires converting file data to bytes
			//Review register_udf() code in this file
			if(!register_udf(as, "udf/updateUserPwd.lua")){
				LOG("UDF registration error.");
				return;
			}
			as_arraylist args;
			as_arraylist_inita(&args, 1);
			as_arraylist_append_str(&args, password);

			as_val* return_val = NULL;

			if (aerospike_key_apply(as, &err, NULL, &key, "updateUserPwd",
					"updatePassword", (as_list*)&args, &return_val) != AEROSPIKE_OK) {
				LOG("aerospike_key_apply() returned %d - %s", err.code, err.message);
				exit(-1);
			}
			else{
				//Exercise R2
				//Handling as_val *returnVal
				printf("\nINFO: User password changed to %s\n",
						as_val_tostring(return_val) );
			}

		}
		else{
			printf("\nINFO: Invalid User password entered!\n");
		}
	}
}

void aggregateUsersByTweetCountByRegion(aerospike *as){
	int min;
	int max;

	as_error err;

	printf("\n********** Query Users By Tweet Count Range **********\n");

	printf("\nEnter Min Tweet Count:");
	scanf("%d",&min);

	printf("\nEnter Max Tweet Count:");
	scanf("%d",&max);


	if ( max >= min ){

		//Exercise A2
		//Review code in register_udf(). C Client requires
		//UDF file content be uploaded in as_bytes to the server
		//The .lua file must be manually copied to the client side user_path

		if(!register_udf(as, "udf/aggregationByRegion.lua")){
			LOG("UDF registration error.");
			return;
		}
		as_query query;
		as_query_init(&query, "test", "users");
		as_query_where_inita(&query, 1);
		as_query_where(&query, "tweetcount", as_integer_range(min, max));

		as_query_apply(&query, "aggregationByRegion", "sum", NULL);
		if (aerospike_query_foreach(as, &err, NULL, &query, streamUDF_cb, NULL) !=
				AEROSPIKE_OK) {
			LOG("aerospike_query_foreach() returned %d - %s\n", err.code,
					err.message);
			as_query_destroy(&query);
			exit(-1);
		}

		LOG("map-reduce query executed");
		as_query_destroy(&query);
	}
}


bool
register_udf(aerospike* p_as, const char* udf_file_path)
{


	FILE* file = fopen(udf_file_path, "r");

	if (! file) {
		LOG("cannot open script file %s \n", udf_file_path);
		return false;
	}

	uint8_t* content = (uint8_t*)malloc(1024 * 1024);

	if (! content) {
		LOG("script content allocation failed\n");
		fclose(file);
		return false;
	}

	uint8_t* p_write = content;
	int read = (int)fread(p_write, 1, 512, file);
	int size = 0;

	while (read) {
		size += read;
		p_write += read;
		read = (int)fread(p_write, 1, 512, file);
	}

	fclose(file);


	as_bytes udf_content;
	as_bytes_init_wrap(&udf_content, content, size, true);

	as_error err;
	as_string base_string;
	const char* base = as_basename(&base_string, udf_file_path);

	if (aerospike_udf_put(p_as, &err, NULL, base, AS_UDF_TYPE_LUA,
			&udf_content) == AEROSPIKE_OK) {
		aerospike_udf_put_wait(p_as, &err, NULL, base, 100);
	}
	else {
		LOG("aerospike_udf_put() returned %d - %s\n", err.code, err.message);
	}

	as_string_destroy(&base_string);
	as_bytes_destroy(&udf_content);

	return err.code == AEROSPIKE_OK;
}

int split( char *str, const char *delim, char *outlist[] ) {
	char    *tk;
	int     cnt = 0;

	tk = strtok( str, delim );
	while( tk != NULL && cnt < 256 ) {
		outlist[cnt++] = tk;
		tk = strtok( NULL, delim );
	}
	return cnt;
}


//Exercise A2
//Callback function
bool
streamUDF_cb(const as_val* p_val, void* udata) {
	if (! p_val) {
		LOG("Stream UDF callback returned null - aggregation is complete.\n");
		return true;
	}
	if (! as_map_fromval(p_val)) {
		LOG("Stream UDF callback returned non-as_map object.\n");
		return true;
	}
	char* val_as_str = as_val_tostring(p_val);

	LOG("Stream UDF callback returned %s\n", val_as_str);
	free(val_as_str);

	return true;
}
