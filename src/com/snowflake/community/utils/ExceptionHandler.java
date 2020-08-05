/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.utils;

import java.util.Objects;

public class ExceptionHandler {

	/**
	 * Returns the root cause of an exception
	 * @param throwable The thrown exception
	 * @return The root cause of the exception
	 */
	public static Throwable getRootCause(Throwable throwable) {
	    Objects.requireNonNull(throwable);
	    Throwable rootCause = throwable;
	    while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
	        rootCause = rootCause.getCause();
	    }
	    return rootCause;
	}
	
	/**
	 * Returns the exception message for a root cause
	 * @param throwable The root cause of an exception
	 * @return The message for the root cause 
	 */
	public static String getRootCauseMessage(Throwable throwable) {
		return getRootCause(throwable).getMessage();
	}
	
}
