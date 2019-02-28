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

/**
 * Autoloader class for classes in the \Aerospike\Training namespace.
 */
class Autoloader
{
    /**
     * Call to register the Aerospike\LDT\Autoloader::load() method
     */
    public static function register()
    {
        spl_autoload_register(array(__CLASS__, 'load'));
    }

    /**
     * The loading function for a $class_name in the \Aerospike\LDT namespace
     *
     * @param string $class_name
     */
    public static function load($class_name)
    {
        $parts = explode('Aerospike\\Training\\', $class_name);
        require __DIR__. DIRECTORY_SEPARATOR. $parts[1]. '.php';
    }
}

?>
