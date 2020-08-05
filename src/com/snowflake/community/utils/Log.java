/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.utils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Log {
	
	private static boolean _interactiveMode = true;
	
	public static void clearConsole() {
		System.out.print("\033[H\033[2J");  // May not work for Windows
	}
	
	
	/**
	 * Logs a message to stdout with timestamp
	 * @param logEntry The message to log
	 */
	public static void log(String logEntry) {
		System.out.println(getTimestamp() + logEntry);
	}
	
	/**
	 * Logs a message to stdout without timestamp
	 * @param logEntry
	 */
	public static void out(String logEntry) {
		if (_interactiveMode) {
			System.out.println(logEntry);	
		}
	}
	
	/**
	 * Logs an exception to standard out
	 * @param e The exception to log
	 */
	public static void logException(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        String exceptionAsString = sw.toString();
        System.out.println(getTimestamp() + exceptionAsString);
	}
	
	/**
	 * Gets the current timestamp
	 * @return The current timestamp
	 */
	private static String getTimestamp() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
		return df.format(new Date()) + ": ";
	}

	/**
	 * Returns if logging is in interactive mode
	 * @return
	 */
	public static boolean isInteractiveMode() {
		return _interactiveMode;
	}

	/**
	 * Sets if logging is in interactive mode
	 * @param _interactiveMode
	 */
	public static void setInteractiveMode(boolean interactiveMode) {
		_interactiveMode = interactiveMode;
	}
}
