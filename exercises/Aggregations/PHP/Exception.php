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

class Exception extends \Exception {
    /**
     * The error message for the last operation
     * @var string
     * @see Aerospike::error()
     */
    protected $error;

    /**
     * Constructor
     *
     * @param Aerospike $client
     * @param string $comment
     */
    public function __construct(Aerospike $client, $comment) {
        $this->error = $client->error();
        parent::__construct($comment, $client->errorno());
    }

    /**
     * Returns the original client error message
     * @return string
     */
    public function getError() {
        return $this->error;
    }
}








?>
