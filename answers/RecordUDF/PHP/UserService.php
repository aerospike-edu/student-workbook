<?php
/*******************************************************************************
 * Copyright 2012-2015 by Aerospike.
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
 *
 * @category   Training
 * @author     Ronen Botzer <rbotzer@aerospike.com>
 * @copyright  Copyright 2012-2015 Aerospike, Inc.
 * @filesource
 */
namespace Aerospike\Training;
use Aerospike;

class UserService extends BaseService {

    /**
     * Constructor
     *
     * @param Aerospike $client
     * @param array $config
     */
    public function __construct(Aerospike $client, array $config, $annotate = false) {
        parent::__construct($client, $config, $annotate);
    }

    /* Data Model
     * Namespace: test
     * Set: users
     * Key: <username>
     * Bins:
     * username - String
     * password - String (For simplicity password is stored in plain-text)
     * gender - String (Valid values are 'm' or 'f')
     * region - String (Valid values are: 'n' (North), 's' (South), 'e' (East), 'w' (West) -- to keep data entry to minimal we just store the first letter)
     * lasttweeted - int (Stores epoch timestamp of the last / most recent tweet) -- Default to 0
     * tweetcount - int (Stores total number of tweets for the user) -- Default to 0
     * interests - Array of interests
     * Sample Key: dash
     * Sample Record:
     * { username: 'dash',
     *   password: 'dash',
     *   gender: 'm',
     *   region: 'w',
     *   lasttweeted: 1408574221,
     *   tweetcount: 20,
     *   interests: ['photography', 'technology', 'dancing', 'house music]
     * }
     */

    /**
     * Asks the developer for input and creates a new user in the database
     * as a record in namespace 'test', set 'users'
     * @return string|null the username that was just created
     * @throws \Aerospike\Training\Exception
     */
    public function createUser() {
        echo colorize("\nCreate a user\n", 'blue', true);
        // Get username
        echo colorize("Enter username (or hit Return to skip): ");
        $username = trim(readline());
        if ($username == '') return;
        $bins = array('username' => $username);

        // Get password
        echo colorize("Enter password for $username: ");
        $bins['password'] = trim(readline());

        // Get gender
        echo colorize("Select gender (f or m) for $username: ");
        $bins['gender'] = substr(trim(readline()), 0, 1);

        // Get region
        echo colorize("Select region (north, south, east or west) for $username: ");
        $bins['region'] = substr(trim(readline()), 0, 1);

        // Get interests
        echo colorize("Enter comma-separated interests for  $username: ");
        $bins['interests'] = explode(',', trim(readline()));

        /* using the default OPT_POLICY_EXISTS value POLICY_EXISTS_IGNORE
         * (aka POLICY_CREATE_OR_UPDATE) which creates a new record at the
         * given key, or otherwise updates it with bin values
         */
        echo colorize("Creating user record ≻", 'black', true);
        $key = $this->getUserKey($username);
        if ($this->annotate) display_code(__FILE__, __LINE__, 6);
        $status = $this->client->put($key, $bins);
        if ($status !== Aerospike::OK) {
            // throwing an \Aerospike\Training\Exception
            echo fail();
            throw new Exception($this->client, "Failed to create user $username");
        }
        echo success();
        return $username;
    }

    /**
     * Asks the developer for input and updates a user's password using the
     * check-and-set pattern
     * @throws \Aerospike\Training\Exception
     */
    public function updatePasswordUsingCAS() {
        echo colorize("\nUpdate a user's password using CAS\n", 'blue', true);
        // Get username
        echo colorize("Enter username (or hit Return to skip): ");
        $username = trim(readline());
        if ($username == '') return;

        // Get password
        echo colorize("Enter a new password for $username: ");
        $new_password = trim(readline());

        echo colorize("Getting the metadata for the record ≻", 'black', true);
        $key = $this->getUserKey($username);
        if ($this->annotate) display_code(__FILE__, __LINE__, 1);
        $status = $this->client->exists($key, $metadata);
        if ($status !== Aerospike::OK) {
            // throwing an \Aerospike\Training\Exception
            echo fail();
            throw new Exception($this->client, "Failed to retrieve metadata for the record");
        }
        echo success();
        var_dump($metadata);

        echo colorize("Updating the user's password if the generation matches ≻", 'black', true);
        if ($this->annotate) display_code(__FILE__, __LINE__, 4);
        $bins = array('password' => $new_password);
        $policy = array(Aerospike::OPT_POLICY_GEN =>
                    array(Aerospike::POLICY_GEN_EQ, $metadata['generation']));
        $status = $this->client->put($key, $bins, $metadata['ttl'], $policy);
        if ($status !== Aerospike::OK) {
            // throwing an \Aerospike\Training\Exception
            echo fail();
            throw new Exception($this->client, "Writing with POLICY_GEN_EQ failed due to generation mismatch");
        }
        echo success();
    }

    /**
     * Asks the developer for input and updates a user's password using a record UDF
     * @throws \Aerospike\Training\Exception
     */
    public function updatePasswordUsingUDF() {
        echo colorize("\nUpdate a user's password using a UDF\n", 'blue', true);
        // Get username
        echo colorize("Enter username (or hit Return to skip): ");
        $username = trim(readline());
        if ($username == '') return;

        // Get password
        echo colorize("Enter a new password for $username: ");
        $new_password = trim(readline());

        echo colorize("Ensuring the UDF module is registered ≻", 'black', true);
        $ok = $this->ensureUdfModule('udf/updateUserPwd.lua', 'updateUserPwd.lua');
        if ($ok) echo success();
        else echo fail();
        $this->display_module('udf/updateUserPwd.lua');

        echo colorize("Updating the user record ≻", 'black', true);
        $key = $this->getUserKey($username);
        if ($this->annotate) display_code(__FILE__, __LINE__, 7);
        $args = array($new_password);
        $status = $this->client->apply($key, 'updateUserPwd', 'updatePassword', $args);
        if ($status !== Aerospike::OK) {
            // throwing an \Aerospike\Training\Exception
            echo fail();
            throw new Exception($this->client, "Failed to update password for user $username");
        }
        echo success();
    }

    /**
     * Asks the developer for input and finds the aggregated users by region
     * that fit a tweet count range
     * @throws \Aerospike\Training\Exception
     */
    public function aggregateUsersByRegion() {
        echo colorize("\nAggregate user's by region whose tweet count is in a given range", 'blue', true)."\n";
        echo colorize("This is part of the Aggregations exercises", 'red', true)."\n";
    }

}

?>
