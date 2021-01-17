/*
 * Copyright (c) 2020, 2021 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.jdbc;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;

import com.snowflake.community.utils.Log;

import net.snowflake.client.jdbc.SnowflakeDriver;

public class SnowflakeConnection {

	private static final int HTTP_MOVED_PERMANENTLY = 301;
	private static final int HTTP_OK = 200;

	private Connection con;
	private String accountURL = null;
	private String user = null;
	private String password = null;
	private String role = null;
	private String warehouse = null;
	private Properties extraProps = null;
	
	/**
	 * Checks to see if an untested Snowflake URL responds to HTTP requests 
	 * @param snowflakeURL The URL to test
	 * @return True if the URL responds properly to HTTP requests, false otherwise
	 * @throws IOException
	 */
	private boolean checkSnowflakeURL(String snowflakeURL) throws IOException {
		if (!snowflakeURL.trim().toLowerCase().endsWith("snowflakecomputing.com")) {
			throw new IllegalArgumentException("Snowflake connection URL must end with snowflakecomputing.com");
		}
		Log.log("Testing http connection to Snowflake account at URL: " + snowflakeURL);
		int httpResponse;
		// Test HTTP for a 301 status:
		URL url = new URL("http://" + snowflakeURL);
		HttpURLConnection huc = (HttpURLConnection) url.openConnection();
		httpResponse = huc.getResponseCode();
		Log.log("Http ResponseCode = " + httpResponse);
		if(httpResponse != HTTP_MOVED_PERMANENTLY && httpResponse != HTTP_OK) {
			throw new IllegalArgumentException("Invalid Snowflake URL");
		}
		// Do NOT test HTTPS if the URL has an underscore:
		if(snowflakeURL.indexOf("_") == -1) {
			// Test HTTP for a 200 status:
			url = new URL("https://" + snowflakeURL);
			huc = (HttpURLConnection) url.openConnection();
			httpResponse = huc.getResponseCode();
			Log.log("Http ResponseCode = " + httpResponse);
			if(httpResponse != HTTP_OK) {
				throw new IllegalArgumentException("Invalid Snowflake URL");
			}
		}			
		return true;
	}
	
	/**
	 * Adds extra connection properties; use only if required.
	 * https://docs.snowflake.net/manuals/user-guide/jdbc-configure.html
	 * @param props a Property object containing all required connection properties
	 */
	public void setConnectionProperties(Properties props) {
		extraProps = props;
	}
	
	/**
	 * Gets a list of extra connection properties.
	 * https://docs.snowflake.net/manuals/user-guide/jdbc-configure.html
	 * @return a list of extra connection properties
	 */
	public Properties getConnectionProperties() {
		return extraProps;
	}
	
	/**
	 * Sets the Snowflake account URL. Use a fully-qualified name without protocol prefix such as:
	 * myaccount.us-east-1.snowflakecomputing.com
	 * @param accountFullURL the full account URL except the HTTPS:// prefix
	 */
	public void setAccountURL(String accountFullURL) {
		accountURL = accountFullURL;
		try {
			close();
		} catch (SQLException e) {
			Log.logException(e);
		}
	}
	
	/**
	 * Gets the Snowflake account URL
	 * @return the full account URL except the HTTPS:// prefix
	 */
	public String getAccountURL() {
		return accountURL;
	}
	
	/** 
	 * Sets the user name for the Snowflake connection
	 * @param userName the user name for the connection
	 */
	public void setUser(String userName) {
		user = userName;
		try {
			close();
		} catch (SQLException e) {
			Log.logException(e);
		}
	}
	
	/**
	 * Gets the user name for the Snowflake connection
	 * @return the user name for the connection
	 */
	public String getUser() {
		return user;
	}
	
	/**
	 * Sets the password used for the Snowflake connection
	 * @param snowflakePassword the password for the connection
	 */
	public void setPassword(String snowflakePassword) {
		password = snowflakePassword;
		try {
			close();
		} catch (SQLException e) {
			Log.logException(e);
		}
	}
	
	/**
	 * Gets the password used for the Snowflake connection
	 * @return the password for the connection
	 */
	public String getPassword() {
		return password;
	}
	
	/**
	 * Sets the Snowflake role to use; recommended role is SYSADMIN
	 * @param userRole the role to use
	 */
	public void setRole(String userRole) {
		role = userRole;
	}
	
	/**
	 * Gets the Snowflake role in use
	 * @return the role in use
	 */
	public String getRole() {
		return role;
	}	

	/**
	 * Sets the Snowflake warehouse to use; recommended to use extra small with auto-suspend enabled 
	 * @param warehouseName the warehouse to use
	 */
	public void setWarehouse(String warehouseName) {
		warehouse = warehouseName;
	}
	
	/**
	 * Gets the Snowflake warehouse in use
	 * @return
	 */
	public String getWarehouse() {
		return warehouse;
	}
	
	/**
	 * Gets the connection to Snowflake and attempts to connect
	 * @return the Snowflake connection, if not connected will attempt to connect
	 * @throws IOException 
	 */
    public Connection getConnection() throws IllegalArgumentException, SQLException, IOException{
    
    if (accountURL == null || accountURL.trim().length() == 0){throw new IllegalArgumentException("Must set Account URL using setAccountURL."); }
    if (user == null || user.trim().length() == 0) { throw new IllegalArgumentException("Must set user using setUser."); } 
    if (password == null || user.trim().length() == 0) { throw new IllegalArgumentException("Must set password using setPassword."); }
    	    
    if (connect()) {
        	return con;	
    	}
    	else {
    		return null;
    	}
    }
	
	/**
     * Connects to Snowflake via JDBC
     * @return JDBC Connection to the specified Snowflake account
     * @throws SQLException when not able to connect
	 * @throws IOException 
     */
	private Connection getSnowflakeConnection() throws SQLException, IOException {
		DriverManager.registerDriver(new SnowflakeDriver());
		Properties props = new Properties();
		props.put("user", user);
		props.put("password", password);
		props.put("insecureMode", "true"); 		//Turn off OCSP check by default.
		if (extraProps != null) {
			Set<String> keys = extraProps.stringPropertyNames();
		    for (String key : keys) {
		        props.put(key, extraProps.getProperty(key));
		    }
		}
	    // Before trying to connect, test the URL:
	    checkSnowflakeURL(accountURL);
	    Log.log("Opening JDBC connection to " + accountURL);
	    Connection con = DriverManager.getConnection("jdbc:snowflake://" + accountURL, props);
		Log.log("JDBC connection opened to " + accountURL);
		return con;
	}
	
	/**
	 * Checks for an existing connection in the "con" member Connection. If not connected, try to connect
	 * @return true if connected or false if not able to connect
	 * @throws SQLException 
	 * @throws IOException 
	 */
	private boolean connect() throws SQLException, IOException {
		if (isConnected()) {
			return true; 
		}
		else {	
			con = getSnowflakeConnection();
			return true;
		}
	}
	
	/**
	 * Checks to see if the member Connection "con" is connected.
	 * @return true if connected, false if not connected or error checking
	 * @throws SQLException 
	 */
	public boolean isConnected() throws SQLException {
		if (con == null) {
			return false;
		}
		else {
			return con.isValid(15);
		}
	}
	
	/**
	 * Closes the connection to Snowflake
	 * @throws SQLException
	 */
	private void close() throws SQLException {
		if (con != null) {
			con.close();	
		}
	}
}
