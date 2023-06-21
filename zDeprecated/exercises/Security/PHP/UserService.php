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

class UserService extends BaseService
{

    /**
     * Constructor
     *
     * @param Aerospike $client
     * @param array $config
     */
    public function __construct(Aerospike $client, array $config, $annotate = false)
    {
        parent::__construct($client, $config, $annotate);
    }

    public function createUser()
    {
        echo colorize("\nCreate user\n", 'blue', true);
        // Get username
        echo colorize("Enter username: ");
        $username = trim(readline());
        if ($username == '') return;

        // Get password
        echo colorize("Enter password for $username: ");
        $password = trim(readline());

        // Get role
        echo colorize("Enter a role for $username: ");
        $role = trim(readline());

        echo colorize("Creating user ≻", 'black', true);
        $key = $this->getUserKey($username);
        if ($this->annotate) display_code(__FILE__, __LINE__, 6);
        // @todo Exercise 2
        // @todo Create User
        if ($status !== Aerospike::OK) {
            // throwing an \Aerospike\Training\Exception
            echo fail();
            throw new Exception($this->client, "Failed to create $username");
        }
        echo success();
        return $username;
    }


    public function getUser()
    {
        // Get username
        echo colorize("Enter username: ");
        $username = trim(readline());
        if ($username == '') return;

        // @todo Exercise 3
        // @todo Read User
        if ($status !== Aerospike::OK) {
            // throwing an \Aerospike\Training\Exception
            echo fail();
            throw new Exception($this->client, "Failed to read user");
        }
        echo success();
        var_dump($returned);
    }


    public function grantRole()
    {
        // Get username
        echo colorize("Enter username: ");
        $username = trim(readline());
        if ($username == '') return;

        // Get role
        echo colorize("Enter a role for $username: ");
        $role = trim(readline());

        // @todo Exercise 4
        // @todo Grant Role to User
        if ($status !== Aerospike::OK) {
            // throwing an \Aerospike\Training\Exception
            echo fail();
            throw new Exception($this->client, "Failed to grant role");
        }
        echo success();
        var_dump($status);
    }

    public function revokeRole()
    {
        // Get username
        echo colorize("Enter username: ");
        $username = trim(readline());
        if ($username == '') return;

        // Get role
        echo colorize("Enter a role to revoke from $username: ");
        $role = trim(readline());

        // @todo Exercise 5
        // @todo Revoke Role from User
        if ($status !== Aerospike::OK) {
            // throwing an \Aerospike\Training\Exception
            echo fail();
            throw new Exception($this->client, "Failed to revoke role");
        }
        echo success();
        var_dump($status);
    }


    public function dropUser()
    {
        // Get username
        echo colorize("Enter username: ");
        $username = trim(readline());
        if ($username == '') return;


        // @todo Exercise 6
        // @todo Drop User
        if ($status !== Aerospike::OK) {
            // throwing an \Aerospike\Training\Exception
            echo fail();
            throw new Exception($this->client, "Failed to drop user");
        }
        echo success();
        var_dump($status);
    }
}
?>
