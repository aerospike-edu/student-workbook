/*******************************************************************************
 * Copyright 2012-2014 by Aerospike.
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

package com.aerospike.developer.training;

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;


public class EclipseConsole {
	Console systemConsole = System.console();
	boolean useSystemConsole = false;

	public EclipseConsole(){
		this.useSystemConsole = (this.systemConsole != null);
	}

	public void printf(String message){
		if (useSystemConsole)
			systemConsole.printf(message);
		else {
			System.out.printf(message);
		}
	}

	

	public String readLine(){
		if (useSystemConsole)
			return systemConsole.readLine();
		else {
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
			String line = "";
			try {
				line = bufferedReader.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return line;
		}
	}

}
