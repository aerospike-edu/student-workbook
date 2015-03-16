<?php
/*
 * You can include this file and it will handle registering the autoloader.
 */
$autoloaders = spl_autoload_functions();
if (!$autoloaders || !array_key_exists('Aerospike\\Training\\Autoloader', $autoloaders)) {
    require __DIR__. '/Autoloader.php';
    \Aerospike\Training\Autoloader::register();
}

?>
