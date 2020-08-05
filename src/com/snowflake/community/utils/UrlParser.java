/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.utils;

public class UrlParser {

	public static String SnowflakeAccountFromURL(String snowflakeURL) {
		
		String s = snowflakeURL.trim().toLowerCase();
		
		if(s.contains("//")) {
			s = s.split("//")[1];
		}
		
		if(s.contains("/")) {
			s = s.split("/")[0];
		}
		
		if(!s.endsWith(".snowflakecomputing.com")) {
			s += ".snowflakecomputing.com";
		}
		return s;
	}
}
