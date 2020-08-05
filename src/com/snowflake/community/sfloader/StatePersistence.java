/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.sfloader;

public class StatePersistence {

	/**
	 * Loads values from the Sqlite database (if it exists) into StateControl
	 */
	public static void loadStateFromDatabase() {
		StateControl.setSnowflakePassword	 (SqliteManager.getEncryptedProperty	(StateControl.SNOWFLAKE_PASSWORD      ));
		StateControl.setLocalDirectory		 (SqliteManager.getStoredProperty		(StateControl.LOCAL_FILE_DIRECTORY	  ));
		StateControl.setSnowflakeAccount	 (SqliteManager.getStoredProperty		(StateControl.SNOWFLAKE_ACCOUNT		  ));
		StateControl.setSnowflakeLogin		 (SqliteManager.getStoredProperty		(StateControl.SNOWFLAKE_LOGIN		  ));
		StateControl.setDatabase			 (SqliteManager.getStoredProperty		(StateControl.SNOWFLAKE_DATABASE	  ));
		StateControl.setSchema				 (SqliteManager.getStoredProperty		(StateControl.SNOWFLAKE_SCHEMA		  ));
		StateControl.setStage				 (SqliteManager.getStoredProperty		(StateControl.SNOWFLAKE_STAGE		  ));
		StateControl.setPutStatement		 (SqliteManager.getStoredProperty		(StateControl.PUT_STATEMENT  		  ));		
		StateControl.setThreadCount          (SqliteManager.getStoredProperty		(StateControl.THREAD_COUNT  		  ));
		StateControl.setSourceCompression    (SqliteManager.getStoredProperty		(StateControl.SOURCE_COMPRESSION      ));
		StateControl.setOverwriteExisting    (SqliteManager.getStoredProperty		(StateControl.OVERWRITE_EXISTING      ));
		StateControl.setPositiveRegex        (SqliteManager.getStoredProperty		(StateControl.POSITIVE_REGEX          ));
		StateControl.setNegativeRegex        (SqliteManager.getStoredProperty		(StateControl.NEGATIVE_REGEX          ));
		StateControl.setDoNotStoreCredentials(SqliteManager.getStoredProperty   	(StateControl.DO_NOT_STORE_CREDENTIALS));
	}
}