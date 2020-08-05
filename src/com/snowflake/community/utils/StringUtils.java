/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.utils;

public class StringUtils {

	/**
	 * Returns a string from an object that if null returns an empty string instead of null
	 * @param o The object to convert to a null-safe string
	 * @return The string representation of the object or an empty string if null
	 */
	public static String getNullSafeString(Object o) {	
		if(o == null) return "";
		return o.toString();
	}
	
	/**
	 * Returns whether a string is numeric
	 * @param strNum The string to check to see if it's numeric
	 * @return True if the string is numeric, false otherwise
	 */
	public static boolean isNumeric(String strNum) {
	    if (strNum == null) {
	        return false;
	    }
	    try {
			Double.parseDouble(strNum);
	    } catch (NumberFormatException e) {
	        return false;
	    }
	    return true;
	}
}
