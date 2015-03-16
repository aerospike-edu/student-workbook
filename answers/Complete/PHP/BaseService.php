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

abstract class BaseService {
    /**
     * The database client config
     * @var array
     */
    protected $config;
    /**
     * The database client class
     * @var Aerospike
     */
    protected $client;

    /**
     * Constructor
     *
     * @param Aerospike $client
     */
    public function __construct(Aerospike $client, $config, $annotate = false) {
        $this->annotate = (boolean) $annotate;
        $this->config = $config;
        $this->client = $client;
    }

    /**
     * Asks the developer for the username then fetches the matching record
     * from the database using tuple ('test', 'users', $username)
     * @param string|null $username optionally provide the username
     * @return array
     * @throws \Aerospike\Training\Exception
     */
    public function getUser($username = null) {
        if ($username == '') {
            // Get username
            echo colorize("Enter username (or hit Return to skip): ");
            $username = trim(readline());
        }
        if ($username != '') {
            $key = $this->getUserKey($username);
            if ($this->annotate) display_code(__FILE__, __LINE__, 6);
            $status = $this->client->get($key, $record);
            if ($status !== Aerospike::OK) {
                // throwing an \Aerospike\Training\Exception
                throw new Exception($this->client, "Failed to get the user $username");
            }
            return $record;
        } else {
            throw new \Exception("Invalid input provided for username in UserService::getUser()");
        }
    }

    /**
     * Gets the database key for the user based on the username
     * @param string $username optionally provide the username
     * @return array
     */
    protected function getUserKey($username) {
        if ($this->annotate) display_code(__FILE__, __LINE__, 1);
        $key = $this->client->initKey('test', 'users', $username);
        return $key;
    }

    /**
     * Gets the database key for a tweet
     * @param string $username optionally provide the username
     * @param int $id
     * @return array
     */
    protected function getTweetKey($username, $id) {
        return $this->client->initKey('test', 'tweets', $username. ':'. $id);
    }

    /**
     * Checks for the existence of the secondary index and creates it if needed
     * @param string $ns
     * @param string $set
     * @param string $bin
     * @param string $index_name
     * @param int $index_type Aerospike::INDEX_TYPE_STRING or Aerospike::INDEX_TYPE_STRING
     * @return boolean indicating whether the operation succeeded
     */
    protected function ensureIndex($ns, $set, $bin, $index_name, $index_type) {
        if ($this->annotate) display_code(__FILE__, __LINE__, 9);
        $status = $this->client->info("sindex/$ns/$index_name", $response, $this->config);
        if ($status !== Aerospike::OK) {
            $status = $this->client->createIndex($ns, $set, $bin, $index_type, $index_name);
            return ($status === Aerospike::OK);
        } else if (strrpos($response, 'NO INDEX') !== false) {
            $status = $this->client->createIndex($ns, $set, $bin, $index_type, $index_name);
            return ($status === Aerospike::OK);
        }
        return true;
    }

    /**
     * Checks for the existence of the UDF module and registers it if needed
     * @param string $module_path
     * @param string $module_alias
     * @return boolean indicating whether the operation succeeded
     */
    protected function ensureUdfModule($module_path, $module_alias) {
        if ($this->has_module($module_alias)) {
            return true;
        } else {
            if ($this->annotate) display_code(__FILE__, __LINE__, 1);
            return ($this->client->register($module_path, $module_alias) === Aerospike::OK);
        }
    }

    /**
     * Display the contents of the UDF module
     */
    protected function display_module($module_path) {
        // display the Lua code of the UDF module
        $lua = file_get_contents($module_path);
        echo colorize($lua, 'purple', false);
    }

    /**
     * Checks for the existence of the UDF module
     * @param string $module_alias
     * @return boolean
     */
    private function has_module($module_alias) {
        if ($this->annotate) display_code(__FILE__, __LINE__, 6);
        $status = $this->client->listRegistered($modules);
        if ($status !== Aerospike::OK) return false;
        foreach ($modules as $module) {
            if ($module['name'] == $module_alias) return true;
        }
        return false;
    }
}

?>
