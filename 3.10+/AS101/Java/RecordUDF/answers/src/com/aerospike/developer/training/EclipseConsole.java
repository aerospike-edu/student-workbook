/* 
 * Copyright 2012-2015 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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
