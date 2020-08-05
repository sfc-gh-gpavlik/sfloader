/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.utils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;

public class Reflection {
	
	private static String jarDirectory;
	private static boolean isDevEnvironment;
	
	static {
		File userDirectory = new File(System.getProperty("user.dir"));
		try {
			Log.out("Reflection detected user.dir as " + userDirectory.getCanonicalPath());
		} catch (IOException e) {
			Log.logException(e);;
		}
		 
		File classpath = new File(System.getProperty("java.class.path"));
		 
		try {
			Log.out("Reflection detected java.class.path as " + classpath.getCanonicalPath());
		} catch (IOException e) {
			Log.logException(e);
		}
		 
		if (classpath.exists()) {
			try {
				String jarPath = classpath.getCanonicalPath();
				isDevEnvironment = false;
				jarDirectory = getDirectoryFromFilePath(jarPath);
			} catch (IOException e) {
				Log.logException(e);
			}
		} else {
			isDevEnvironment = true;
		}
	}	
	
	/**
	 * Returns the directory containing the JAR file running the application
	 * @return The directory containing the JAR file
	 */
	public static String getJarDirectory() {
		 return jarDirectory;
	} 
	
	/**
	 * Returns whether or not the application is running in the development environment 
	 * @return True if the application is running in the development environment
	 */
	public static boolean isDevelopmentEnvironment() {
		return isDevEnvironment;
	}
	
	/**
	 * Returns an ArrayList of Methods in a class
	 * @param theClass The Class to reflect and return the ArrayList of Methods
	 * @return An ArrayList with all Methods in the Class
	 */
	@SuppressWarnings("rawtypes")
	public static ArrayList<Method> getMethods(Class theClass) {
			ArrayList<Method> l = new ArrayList<>();
	        try {
	            Method[] methods = theClass.getDeclaredMethods();
	            for (int i = 0; i < methods.length; i++) {
	                l.add(methods[i]);
	            }
	        } catch (Exception e) {
	            Log.logException(e);
	        }  
	        return l;		
	}
	
	private static String getDirectoryFromFilePath(String filePath) {
		int lastForwardSlash = filePath.lastIndexOf("/");
		int lastBackslash = filePath.lastIndexOf("\\");
		int lastSlash;
		if(lastForwardSlash > lastBackslash) {
			lastSlash = lastForwardSlash;
		} else {
			lastSlash = lastBackslash;
		}
		return filePath.substring(0, lastSlash);			
	}
}
