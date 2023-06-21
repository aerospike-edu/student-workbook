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

class RoleService extends BaseService {

    /**
     * Constructor
     *
     * @param Aerospike $client
     * @param array $config
     */
    public function __construct(Aerospike $client, array $config, $annotate = false) {
        parent::__construct($client, $config, $annotate);
    }

    public function createRole() {
        echo colorize("Enter role: ");
        $role = trim(readline());
        if ($role == '') return;

        echo colorize("Enter a privilege code\n(0 = read, 1 = read-write, 2 = read-write-udf, 3 = data-admin, 4 = sys-admin, 5 = user-admin): ");
        $code = trim(readline());

        $pc = 0;
        switch($code) {
            case 0: $pc = Aerospike::PRIV_READ; break;
            case 1: $pc = Aerospike::PRIV_READ_WRITE; break; //TODO: doc incorrect; https://github.com/aerospike/aerospike-client-php/blob/master/doc/aerospike.md
            case 2: $pc = Aerospike::PRIV_READ_WRITE_UDF; break;
            case 3: $pc = Aerospike::PRIV_DATA_ADMIN; break;
            case 4: $pc = Aerospike::PRIV_SYS_ADMIN; break;
            case 5: $pc = Aerospike::PRIV_USER_ADMIN; break;
        }

        $privilege = array();
        $privilege["code"] = $pc;
        // @todo Exercise 7
        // @todo Create Role
        if ($status !== Aerospike::OK) {
            // throwing an \Aerospike\Training\Exception
            echo fail();
            throw new Exception($this->client, "Failed to create $role");
        }
        echo success();
    }


    public function readRole() {
        echo colorize("Enter role: ");
        $role = trim(readline());
        if ($role == '') return;

        // @todo Exercise 8
        // @todo Read Role
        if ($status !== Aerospike::OK) {
            // throwing an \Aerospike\Training\Exception
            echo fail();
            throw new Exception($this->client, "Failed to read role");
        }
        echo success();
        var_dump($returned);
    }


    public function grantPrivilege() {
        echo colorize("Enter role: ");
        $role = trim(readline());
        if ($role == '') return;

        echo colorize("Enter a privilege code\n(0 = read, 1 = read-write, 2 = read-write-udf, 3 = data-admin, 4 = sys-admin, 5 = user-admin): ");
        $code = trim(readline());

        $pc = 0;
        switch($code) {
            case 0: $pc = Aerospike::PRIV_READ; break;
            case 1: $pc = Aerospike::PRIV_READ_WRITE; break; //TODO: doc incorrect; https://github.com/aerospike/aerospike-client-php/blob/master/doc/aerospike.md
            case 2: $pc = Aerospike::PRIV_READ_WRITE_UDF; break;
            case 3: $pc = Aerospike::PRIV_DATA_ADMIN; break;
            case 4: $pc = Aerospike::PRIV_SYS_ADMIN; break;
            case 5: $pc = Aerospike::PRIV_USER_ADMIN; break;
        }

        $privilege = array();
        $privilege["code"] = $pc;
        // @todo Exercise 9
        // @todo Grant Privilege

        if ($status !== Aerospike::OK) {
            // throwing an \Aerospike\Training\Exception
            echo fail();
            throw new Exception($this->client, "Failed to grant privilege");
        }
        echo success();
        var_dump($status);
    }


    public function revokePrivilege() {
        echo colorize("Enter role: ");
        $role = trim(readline());
        if ($role == '') return;

        echo colorize("Enter a privilege code\n(0 = read, 1 = read-write, 2 = read-write-udf, 3 = data-admin, 4 = sys-admin, 5 = user-admin): ");
        $code = trim(readline());

        $pc = 0;
        switch($code) {
            case 0: $pc = Aerospike::PRIV_READ; break;
            case 1: $pc = Aerospike::PRIV_READ_WRITE; break; //TODO: doc incorrect; https://github.com/aerospike/aerospike-client-php/blob/master/doc/aerospike.md
            case 2: $pc = Aerospike::PRIV_READ_WRITE_UDF; break;
            case 3: $pc = Aerospike::PRIV_DATA_ADMIN; break;
            case 4: $pc = Aerospike::PRIV_SYS_ADMIN; break;
            case 5: $pc = Aerospike::PRIV_USER_ADMIN; break;
        }

        $privilege = array();
        $privilege["code"] = $pc;
        // @todo Exercise 10
        // @todo Revoke Privilege

        if ($status !== Aerospike::OK) {
            // throwing an \Aerospike\Training\Exception
            echo fail();
            throw new Exception($this->client, "Failed to revoke privilege");
        }
        echo success();
        var_dump($status);
    }

    public function dropRole()
    {
        // Get username
        echo colorize("Enter role: ");
        $role = trim(readline());
        if ($role == '') return;


        // @todo Exercise 11
        // @todo Drop Role
        if ($status !== Aerospike::OK) {
            // throwing an \Aerospike\Training\Exception
            echo fail();
            throw new Exception($this->client, "Failed to drop role");
        }
        echo success();
        var_dump($status);
    }
}

?>
