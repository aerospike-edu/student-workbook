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
 ******************************************************************************/
require_once(realpath(__DIR__ . '/training_util.php'));
require_once(realpath(__DIR__ . '/autoload.php'));

function show_menu() {
    echo colorize("\nWhat would you like to do:\n", 'blue', true);
    echo colorize("1> Create User\n", 'blue', false);
    echo colorize("2> Read User\n", 'blue', false);
    echo colorize("3> Grant Role to User\n", 'blue', false);
    echo colorize("4> Revoke Role from User\n", 'blue', false);
    echo colorize("5> Drop User\n", 'blue', false);
    echo colorize("6> Create Role\n", 'blue', false);
    echo colorize("7> Read Role\n", 'blue', false);
    echo colorize("8> Grant Privilege to Role\n", 'blue', false);
    echo colorize("9> Revoke Privilege from Role\n", 'blue', false);
    echo colorize("10> Drop Role\n", 'blue', false);
    echo colorize("0> Exit\n", 'blue', false);
    echo colorize("\nSelect 0-10 and hit enter: ", 'blue', false);
    return intval(trim(readline()));
}

function parse_args() {
    $shortopts  = "";
    $shortopts .= "h::";  /* Optional host */
    $shortopts .= "p::";  /* Optional port */
    $shortopts .= "a";    /* Optionally annotate output with code */

    $longopts  = array(
        "host::",         /* Optional host */
        "port::",         /* Optional port */
        "annotate",       /* Optionally annotate output with code */
        "help",           /* Usage */
    );
    $options = getopt($shortopts, $longopts);
    return $options;
}


$args = parse_args();
if (isset($args["help"])) {
    echo colorize("php program.php [-hHOST] [-pPORT] [-a]\n", 'black', true);
    echo " or\n";
    echo colorize("php program.php [--host=HOST] [--port=PORT] [--annotate]\n", 'black', true);
    global $has_pygmentize;
    if (!$has_pygmentize) {
        echo "for syntax highlighting please install Pygmentize\n";
    }
    exit(1);
}
$HOST_ADDR = (isset($args["h"])) ? (string) $args["h"] : ((isset($args["host"])) ? (string) $args["host"] : "127.0.0.1");
$HOST_PORT = (isset($args["p"])) ? (integer) $args["p"] : ((isset($args["port"])) ? (string) $args["port"] : 3000);
if (isset($args['a']) || isset($args['annotate'])) $annotate = true;
else $annotate = false;

echo colorize("***** Welcome to Aerospike Developer Training *****\n", 'blue', true);
echo colorize("Connecting to Aerospike cluster ≻", 'black', true);
if ($annotate) display_code(__FILE__, __LINE__, 7);
// @todo Exercise 1
// @todo Connect to server

$client = new Aerospike($config, false);
if (!$client->isConnected()) {
    echo standard_fail($client);
    echo colorize("Connection to Aerospike cluster failed! Please check the server settings and try again!\n", 'red', true);
    exit(2);
}
echo success();

$selection = show_menu();
if ($selection === 0) {
    $client->close();
    exit(0);
}
$user_service = new \Aerospike\Training\UserService($client, $config, $annotate);
$role_service = new \Aerospike\Training\RoleService($client, $config, $annotate);
try {
    switch ($selection) {
        case 1:
            $user_service->createUser();
            break;
        case 2:
            $user_service->getUser();
            break;
        case 3:
            $user_service->grantRole();
            break;
        case 4:
            $user_service->revokeRole();
            break;
        case 5:
            $user_service->dropUser();
            break;
        case 6:
            $role_service->createRole();
            break;
        case 7:
            $role_service->readRole();
            break;
        case 8:
            $role_service->grantPrivilege();
            break;
        case 9:
            $role_service->revokePrivilege();
            break;
        case 10:
            $role_service->dropRole();
            break;
        default:
            echo colorize("Invalid selection ($selection)\n", 'red', false);
            $client->close();
            exit(3);
    }
} catch (\Aerospike\Training\Exception $e) {
    echo colorize("Error [{$e->getCode()}] {$e->getError()}", 'red', true)."\n";
    echo colorize($e->getMessage(), 'red', true)."\n";
    var_dump($e->getTraceAsString());
    $client->close();
    exit(4);
} catch (\Exception $e) {
    echo colorize($e->getMessage(), 'red', true)."\n";
    var_dump($e->getTraceAsString());
    $client->close();
    exit(5);
}

$client->close();
?>
