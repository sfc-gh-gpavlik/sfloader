/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.utils;

public class OperatingSystem {

	private static final String OS = System.getProperty("os.name").toLowerCase();
	
	/**
	 * Detects if the running operating system is Microsoft Windows
	 * @return True if the running OS is Windows, false otherwise
	 */
	public static boolean isWindows() {
		return (OS.indexOf("win") >= 0);
	}

	/**
	 * Detects if the running operating system is Apple macOS
	 * @return True if the running OS is macOS, false otherwise
	 */
	public static boolean isMac() {
		return (OS.indexOf("mac") >= 0);
	}

	/**
	 * Detects if the running operating system is UNIX / Linux
	 * @return True if the running OS is UNIX / Linux, false otherwise
	 */
	public static boolean isUnix() {
		return (OS.indexOf("nix") >= 0 || OS.indexOf("nux") >= 0 || OS.indexOf("aix") > 0 );
	}

	/**
	 * Detects if the running operating system is Oracle Solaris
	 * @return True if the running OS is Solaris, false otherwise
	 */
	public static boolean isSolaris() {
		return (OS.indexOf("sunos") >= 0);
	}
	
	/**
	 * Returns the directory path separator for the running operating system
	 * @return The directory path separator
	 */
	public static String getEscapedPathSeparator() {
		if (OperatingSystem.isWindows()) {
			return "\\\\";
		} else {
			return "/";
		}
	}	
}