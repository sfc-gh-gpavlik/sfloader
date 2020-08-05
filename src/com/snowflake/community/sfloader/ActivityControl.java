/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.sfloader;

import java.sql.SQLException;

/**
 * 
 * @author gpavlik
 * A class to centralize and control all activity
 */
public class ActivityControl {
	
	static FileInventory fileInventory;
	
	/**
	 * Deletes all files from the SQLite database inventory table 
	 * @return True if successful
	 * @throws SQLException
	 */
	public static boolean DeleteFileInventory() throws SQLException {
		StateControl.inventoriedFileCount(0);
		return SqliteManager.execute("delete from FILE_INGESTION_CONTROL");
	}
	
	/**
	 * Inventories files from the local file system to the database.
	 * @return True if successful
	 */
	public static boolean inventoryFiles() {
		fileInventory = new FileInventory();
		fileInventory.start();
		return true;
	}
	
	/**
	 * Indicates that the inventory is running.
	 * @return True if running
	 */
	public static boolean isInventoryRunning() {
		return fileInventory.isAlive();
	}

}
