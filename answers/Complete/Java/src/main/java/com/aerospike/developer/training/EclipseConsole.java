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
