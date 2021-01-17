/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.sfloader;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.snowflake.community.encryption.StringEncryption;
import com.snowflake.community.utils.Log;
import com.snowflake.community.utils.OperatingSystem;
import com.snowflake.community.utils.Reflection;

public class SqliteManager {
	
	// Hard-coded paths for development locations for reflection to use to create or use the SQLite DB.
	// Adjust for your development environment and preferred development runtime database location.
	private static String WIN_IDE_PATH = "C:\\Dev\\eclipse-db\\sfloader";
	private static String MAC_IDE_PATH = "/USERS/Shared/eclipse-db/sfloader";
	
	private static Connection conn = null;
	private static String dbDirectory = null;
	private static String dbPath = null;

	public static void initialize() {				
		if (Reflection.isDevelopmentEnvironment()) {
			Log.out("Reflection detected app is running in the development environment.");
			if(OperatingSystem.isMac()) {
				dbDirectory = MAC_IDE_PATH;
			} else if(OperatingSystem.isWindows()) {
				dbDirectory = WIN_IDE_PATH;
			}
		} else {
			dbDirectory = Reflection.getJarDirectory() + File.separator + "db";
			Log.out("Reflection detected JAR file directory at " + dbDirectory);
		}
		File file = new File(dbDirectory);	
		if (!file.exists()) {
			file.mkdir();
		}			
		dbPath = dbDirectory + File.separator + "snowflake.db";
		file = new File(dbPath);
		Log.out("SQLite database path is " + dbPath);
		if (!file.exists()) {
			try {
				Log.log("Building the SQLite objects on database " + file.getAbsolutePath());
				getConnection();
				CreateDatabase();
				Log.log("SQLite database objects successfully created.");
			} catch (Exception e) {
				Log.logException(e);
			}
		} else {
			Log.out("Found existing SQLite database at " + file.getAbsolutePath());
			try {
				getConnection();
				} 
			catch (SQLException e) {
				Log.logException(e);
			}
		}
	}
	
	public static String getStoredProperty(String key) {
		try {
			connect();
			String sql = "select v from properties where k = ?";
	        PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, key);
			ResultSet rs = pstmt.executeQuery();
			rs.next();
			return rs.getString(1);
		} catch (SQLException e) {
			Log.logException(e);
			return "";
		}	
	}

	public static String getEncryptedProperty(String key) {
		return StringEncryption.decryptString(getStoredProperty(key));
	}
	
	public static void setEncryptedProperty(String key, String value) {
		setStoredProperty(key, StringEncryption.encryptStringToBase64(value));
	}
	
	public static PreparedStatement prepareStatement(String sql) throws SQLException {
		connect();
		return conn.prepareStatement(sql);
	}
	
	public static boolean execute(String sql) throws SQLException {
		connect();
		Statement stmt = conn.createStatement();
		return stmt.execute(sql);
	}
	
	public static boolean execute(PreparedStatement pstmt) throws SQLException {
		connect();
		return pstmt.execute();
	}
	
	public static String getSingleValueQuery(String columnName, String sql) {
		Statement stmt;
		try {
	        connect();
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			rs.next();
			return rs.getString(columnName);
		} catch (SQLException e) {
			e.printStackTrace();
			return "";
		}
	}

	public static boolean setStoredProperty(String key, String value) {
		try {
	        connect();
			String sql = "update properties set v = ? where k = ?";
	        PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setString(2, key);
			pstmt.setString(1, value);
			pstmt.execute();
			return true;
			
		} catch (SQLException e) {
			Log.logException(e);
			return false;
		}		
	}
	
	public static String getDbPath() {
		return dbPath;
	}

	public static void CreateDatabase() throws SQLException {
        connect();
		Log.log("Creating the SQLite database at path " + dbPath);	
		execute("create table PROPERTIES (K text primary key, V text);");
		execute("create index PROPERTIES_K_INDEX on properties (k);");
		execute("-- FILE_INGESTION_CONTROL definition\n" + 
				"CREATE TABLE \"FILE_INGESTION_CONTROL\" (\n" + 
				"    FILE_ID                         TEXT primary key ,\n" + 
				"    FILE_PROTOCOL                   TEXT, \n" + 
				"    FILE_PATH                       TEXT, \n" + 
				"    FILE_NAME                       TEXT, \n" + 
				"    FILE_INGESTION_ORDER            INTEGER, \n" + 
				"    FILE_BYTES                      INTEGER, \n" + 
				"    FILE_LAST_MODIFIED              INTEGER, \n" + 
				"    FILE_HASHCODE                   INTEGER, \n" + 
				"    FILE_COMMENTS                   TEXT, \n" + 
				"    STAGING_STATUS                  TEXT, \n" + 
				"    STAGING_OWNER_ID                TEXT, \n" + 
				"    STAGING_OWNER_SESSION           TEXT, \n" + 
				"    STAGING_TRY_COUNT               INTEGER default 0, \n" + 
				"    STAGING_LAST_START_TIME         INTEGER, \n" + 
				"    STAGING_LAST_END_TIME           INTEGER, \n" + 
				"    STAGING_LAST_ERROR_MESSAGE      TEXT, \n" + 
				"    UNIQUE(FILE_PATH, FILE_NAME) \n" +
				");\n");
			
		execute("insert into PROPERTIES values\n" 	  		+ 
				"('LOCAL_FILE_DIRECTORY', null),\n"   		+ 
				"('POSITIVE_REGEX', null),\n"         		+ 
				"('NEGATIVE_REGEX', null),\n"         		+ 
				"('SNOWFLAKE_ACCOUNT', null),\n" 	  		+ 
				"('SNOWFLAKE_LOGIN', null), \n" 	  		+	
				"('SNOWFLAKE_PASSWORD', null), \n" 	  		+
				"('SNOWFLAKE_DATABASE', null), \n"	  		+
				"('SNOWFLAKE_SCHEMA', null), \n"	  		+
				"('SNOWFLAKE_STAGE', null), \n"		  		+
				"('OVERWRITE_EXISTING', null), \n"	  		+
				"('SOURCE_COMPRESSION', null), \n"    		+
				"('PUT_STATEMENT', null), \n"		  		+
				"('DO_NOT_STORE_CREDENTIALS', 'false'), \n"	+
				"('THREAD_COUNT', '4');");

		execute("create view GET_STAGE_INFO as\n" + 
				"select  count(*)        as FILES_TO_STAGE,\n" + 
				"        sum(FILE_BYTES) as BYTES_TO_STAGE\n" + 
				"from    FILE_INGESTION_CONTROL\n" + 
				"where   (STAGING_STATUS not in ('LOADED', 'SKIPPED') || STAGING_STATUS is null) \n" + 
				"and     STAGING_TRY_COUNT < 3;");	
	}
	
	private static Connection getSQLiteConnection() throws SQLException {	
	    String url = "jdbc:sqlite:" + dbPath;
        conn = DriverManager.getConnection(url);
        Log.out("Connected to SQLite at: " + url);
        return conn;
	}
	
	public static Connection getConnection() throws SQLException {
		if (isConnected()) {
			return conn; 
		}
		else {
			conn = getSQLiteConnection();
			return conn;
		}
	}
	
	public static boolean isConnected() throws SQLException {
		if (conn == null) {
			return false;
		}
		else {
			return conn.isValid(30);
		}
	}
	
	public static void closeConnection() throws SQLException {
		conn.close();
	}

	private static void connect() throws SQLException {
		if (!isConnected()) {
			getConnection();
		}
	}
}
