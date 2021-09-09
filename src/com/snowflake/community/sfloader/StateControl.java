/*
 * Copyright (c) 2020, 2021 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.sfloader;

import java.io.File;

import com.snowflake.community.utils.StringUtils;

public class StateControl {
	
	public static final int DEFAULT_THREAD_COUNT = 4;
	
	public static final String SUPPORTED_COMPRESSION = 
			"| AUTO_DETECT | GZIP | BZ2 | BROTLI | ZSTD | DEFLATE | RAW_DEFLATE | NONE |";
	
	public static final String SNOWFLAKE_LOGIN 			= "SNOWFLAKE_LOGIN";
	public static final String SNOWFLAKE_ACCOUNT 		= "SNOWFLAKE_ACCOUNT";
	public static final String SNOWFLAKE_PASSWORD 		= "SNOWFLAKE_PASSWORD";
	public static final String LOCAL_FILE_DIRECTORY 	= "LOCAL_FILE_DIRECTORY";
	public static final String SNOWFLAKE_DATABASE 		= "SNOWFLAKE_DATABASE";
	public static final String SNOWFLAKE_SCHEMA 		= "SNOWFLAKE_SCHEMA";
	public static final String SNOWFLAKE_STAGE 			= "SNOWFLAKE_STAGE";
	public static final String PUT_STATEMENT        	= "PUT_STATEMENT";
	public static final String THREAD_COUNT         	= "THREAD_COUNT";
	public static final String OVERWRITE_EXISTING   	= "OVERWRITE_EXISTING";
	public static final String SOURCE_COMPRESSION   	= "SOURCE_COMPRESSION";
	public static final String POSITIVE_REGEX			= "POSITIVE_REGEX";
	public static final String NEGATIVE_REGEX			= "NEGATIVE_REGEX";
	public static final String DO_NOT_STORE_CREDENTIALS = "DO_NOT_STORE_CREDENTIALS";
	
	public static final String READY				= "Ready";
	
	private static String 	_database				= null;
	private static String 	_schema					= null;
	private static String 	_stage					= null;
	private static String 	_localDirectory			= null;
	private static String   _putStatement			= null;
	private static int   	_threadCount			= 4;
	private static boolean 	_isConnected			= false;
	private static String 	_snowflakePassword		= null;
	private static String 	_snowflakeLogin			= null;
	private static String 	_snowflakeAccount		= null;
	private static boolean 	_hasCheckedInventory	= false;
	private static String 	_negativeRegex			= null;	
	private static String 	_positiveRegex			= null;
	private static long		_inventoriedFileCount   = 0;
	private static long		_stagedFileCount        = 0;
	private static boolean  _overwriteExisting		= false;
	private static String   _sourceCompression      = null;
	private static boolean  _doNotStoreCredentials  = false;
	
	static {
		_inventoriedFileCount = Long.parseLong(SqliteManager.getSingleValueQuery("FILE_COUNT", "select count(*) as FILE_COUNT from FILE_INGESTION_CONTROL"));
	}
	
	public static String readyToConnectToSnowflake() {
		if (_snowflakeAccount == null || _snowflakeAccount.equals("")) return "You must set the Snowflake account before connecting to Snowflake.";
		if (_snowflakeLogin == null || _snowflakePassword.equals("")) return "You must set the Snowflake password before connecting to Snowflake.";
		if (_snowflakePassword == null || _snowflakePassword.equals("")) return "You must set the Snowflake password before connecting to Snowflake.";
		
		return "Ready";
	}
	
	public static String readyToInventoryFiles() {
		if(_localDirectory == null) return "You must set the directory containing the local files to upload before running an inventory.";
		File dir = new File(_localDirectory);
		if (!dir.exists()) return "You must set a valid directory containing the local files to upload before running an inventory.";
		if (!dir.isDirectory()) return "You must set a valid directory containing the local files to upload.\nYou have specified a file instead of a directory.";
		if (dir.getAbsolutePath().indexOf("'") > 0) return "Directories and filenames must not contain single quotes.";
		
		return READY;
	}
	
	public static String readyForPutGeneration() {
		if (_inventoriedFileCount == 0) return "You must inventory files before putting the files to a Snowflake stage.";
		if (_database == null || _database.equals("")) return "You must select a database before generating the put statement.";
		if (_schema == null || _schema.equals("")) return "You must select a schema before generating the put statement.";
		if (_stage == null || _stage.equals("")) return "You must select a stage before generating the put statement.";
		if (_snowflakeAccount == null || _snowflakeAccount.equals("")) return "You must set the Snowflake account before generating the put statement.";
		if (_snowflakeLogin == null || _snowflakeLogin.equals("")) return "You must set the Snowflake login before generating the put statement.";
		if (_snowflakePassword == null || _snowflakePassword.equals("")) return "You must set the Snowflake password before generating the put statement.";
		if (_sourceCompression == null || _snowflakePassword.equals("")) return "You must set the source file compression type before generating the put statement.";

		return READY;
	}
	
	public static String readyToLoad() {
		if (_inventoriedFileCount == 0) return "You must inventory files before putting the files to a Snowflake stage.";
		if (_database == null || _database.equals("")) return "You must select a database before loading files.";
		if (_schema == null || _schema.equals("")) return "You must select a schema before loading files.";
		if (_stage == null || _stage.equals("")) return "You must select a stage before loading files.";
		if (_putStatement == null || _putStatement.equals("")) return "You must specify a PUT statement before loading files.";

		return READY;
	}
	
	public static boolean getDoNotStoreCredentials() {
		return _doNotStoreCredentials;
	}
	
	public static void setDoNotStoreCredentials(String doNotStoreCredentials) {
		_doNotStoreCredentials =  Boolean.parseBoolean(doNotStoreCredentials);
		SqliteManager.setStoredProperty(DO_NOT_STORE_CREDENTIALS, doNotStoreCredentials);
		if (_doNotStoreCredentials) {
			SqliteManager.setStoredProperty(SNOWFLAKE_PASSWORD, "");
			SqliteManager.setStoredProperty(SNOWFLAKE_LOGIN, "");
		} else {
			SqliteManager.setEncryptedProperty(SNOWFLAKE_PASSWORD, getSnowflakePassword());
			SqliteManager.setStoredProperty(SNOWFLAKE_LOGIN, getSnowflakeLogin());
		}
	}
	
	public static String getDatabase() {
		return _database;
	}
	
	public static void setDatabase(String database) {
		_database = database;
		setPutStatement("");
		SqliteManager.setStoredProperty("SNOWFLAKE_DATABASE", database);
	}
	
	public static String getSchema() {
		return StringUtils.getNullSafeString(_schema);
	}
	
	public static void setSchema(String schema) {
		_schema = schema;
		setPutStatement("");
		SqliteManager.setStoredProperty("SNOWFLAKE_SCHEMA", schema);
	}
	
	public static String getStage() {
		return StringUtils.getNullSafeString(_stage);
	}
	
	public static void setStage(String stage) {
		_stage = stage;
		setPutStatement("");
		SqliteManager.setStoredProperty("SNOWFLAKE_STAGE", stage);
	}
	
	public static String getLocalDirectory() {
		return _localDirectory;
	}
	
	public static void setLocalDirectory(String localDirectory) {
		_localDirectory = localDirectory;
		SqliteManager.setStoredProperty(LOCAL_FILE_DIRECTORY, localDirectory);
	}
	
	public static String getPutStatement() {
		return _putStatement;
	}
	
	public static void setPutStatement(String putStatement) {
		_putStatement = putStatement;
		SqliteManager.setStoredProperty(PUT_STATEMENT, putStatement);
	}
	
	public static int getThreadCount() {
		return _threadCount;
	}
	
	public static void setThreadCount(String threadCount) {
		try {
			_threadCount = Integer.parseInt(threadCount);
		} catch (Exception e) {
			_threadCount = DEFAULT_THREAD_COUNT;
		}
		SqliteManager.setStoredProperty(THREAD_COUNT, threadCount);
	}
	
	public static boolean isConnected() {
		return _isConnected;
	}
	
	public static void connectionStatus(boolean isConnected) {
		_isConnected = isConnected;
	}
	
	public static String getSnowflakePassword() {
		return _snowflakePassword;
	}
	
	public static void setSnowflakePassword(String snowflakePassword) {
		_isConnected = false;
		_snowflakePassword = snowflakePassword;
		if(!_doNotStoreCredentials) {
			SqliteManager.setEncryptedProperty(SNOWFLAKE_PASSWORD, snowflakePassword);
		}
	}
	
	public static String getSnowflakeLogin() {
		return _snowflakeLogin;
	}
	
	public static void setSnowflakeLogin(String snowflakeLogin) {
		_isConnected = false;
		_snowflakeLogin = snowflakeLogin;
		if(!_doNotStoreCredentials) {
			SqliteManager.setStoredProperty(SNOWFLAKE_LOGIN, snowflakeLogin);
		}
	}
	
	public static boolean getOverwriteExisting() {
		return _overwriteExisting;
	}
	
	public static void setOverwriteExisting(String overwriteExisting) {
		_overwriteExisting = Boolean.parseBoolean(overwriteExisting);
		setPutStatement("");
		SqliteManager.setStoredProperty(OVERWRITE_EXISTING, overwriteExisting);		
	}
	
	public static String getSourceCompression() {
		return _sourceCompression;
	}
	
	public static void setSourceCompression(String sourceCompression) {
		if (sourceCompression != null) {
			String compression = sourceCompression.toUpperCase().trim();
			if (SUPPORTED_COMPRESSION.contains("| " + compression + " |")) {
				_sourceCompression = sourceCompression;
				setPutStatement("");
				SqliteManager.setStoredProperty(SOURCE_COMPRESSION, compression);		
			} else {
				throw new IllegalArgumentException("Unsupported source compression type. Use one of: " + SUPPORTED_COMPRESSION);
			}
		}
	}
	
	public static String getSnowflakeAccount() {
		return _snowflakeAccount;
	}
	
	public static void setSnowflakeAccount(String snowflakeAccount) {
		_isConnected = false;
		_snowflakeAccount = snowflakeAccount;
		SqliteManager.setStoredProperty(SNOWFLAKE_ACCOUNT, snowflakeAccount);		
	}
	
	public static long getInventoriedFileCount() {
		return _inventoriedFileCount;
	}
	
	public static void inventoriedFileCount(long inventoriedFileCount) {
		_inventoriedFileCount = inventoriedFileCount;
	}
	
	public static void incrementInventoriedFileCount() {
		_inventoriedFileCount++;
	}
	
	public static boolean isHasCheckedInventory() {
		return _hasCheckedInventory;
	}
	
	public static void hasCheckedInventory(boolean hasCheckedInventory) {
		_hasCheckedInventory = hasCheckedInventory;
	}
	
	public static String getNegativeRegex() {
		if (_negativeRegex == null) _negativeRegex = "";
		return _negativeRegex.trim();
	}
	
	public static void setNegativeRegex(String negativeRegex) {
		SqliteManager.setStoredProperty(NEGATIVE_REGEX, negativeRegex);
		_negativeRegex = negativeRegex;
	}
	
	public static String getPositiveRegex() {
		if (_positiveRegex == null) _positiveRegex = "";
		return _positiveRegex.trim();
	}
	
	public static void setPositiveRegex(String positiveRegex) {
		SqliteManager.setStoredProperty(POSITIVE_REGEX, positiveRegex);
		_positiveRegex = positiveRegex;
	}
	
	public static void stagedFileCount(long stagedFileCount) {
		_stagedFileCount = stagedFileCount;
	}
	
	public static long getStagedFileCount() {
		return _stagedFileCount;
	}
}
