/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.sfloader;

import java.io.File;
import java.sql.PreparedStatement;
import java.util.UUID;
import java.util.regex.Pattern;

import com.snowflake.community.utils.Log;
import com.snowflake.community.utils.OperatingSystem;

public class FileInventory extends Thread{
	
	public boolean hasPositiveRegex;
	public boolean hasNegativeRegex;
	
	public void run() {
		hasPositiveRegex = (StateControl.getPositiveRegex().compareTo("") != 0);
		hasNegativeRegex = (StateControl.getNegativeRegex().compareTo("") != 0);
		Log.log("hasPositiveRegex = " + hasPositiveRegex);
		RunInventory();
	}
	
	private void RunInventory() {
		listFiles(StateControl.getLocalDirectory());
	}

	private void listFiles(String directoryName) {
		
	    File directory = new File(directoryName);
	    File[] fList = directory.listFiles();
	    
	    if (fList == null) return;
	    int f=0;
	    for (File file : fList) {
	    	if (file.isFile()) {
	    		if(!hasPositiveRegex || Pattern.matches(StateControl.getPositiveRegex(), file.getName())) {
	    			if(!hasNegativeRegex || !Pattern.matches(StateControl.getNegativeRegex(), file.getName())) {
	    	    		Log.log("Inventorying file " + file.getAbsolutePath() + ", " + file.length() + ", " + 
	    	    				file.lastModified() + ", " + file.hashCode());		
	    	    		f = f + fileToDB(file);
	    			}
	    		}
	    	} else if (file.isDirectory()) {
	            listFiles(file.getAbsolutePath());
	        }
	    }
	    Log.log("Inventoried " + f + " files.");
	    return;
	} 
	
	private int fileToDB(File f) {
		
		String fileParent;
		if (OperatingSystem.isWindows()) {
			fileParent = f.getParent().replace("\\", "/");
		} else {
			fileParent = f.getParent();
		}
		
		try {
			String uuid = UUID.randomUUID().toString();
			String sql = "insert into FILE_INGESTION_CONTROL "
					+ "(FILE_ID, FILE_PATH, FILE_NAME, FILE_INGESTION_ORDER, FILE_BYTES, FILE_LAST_MODIFIED, FILE_HASHCODE ) values"
					+ "(?,?,?,?,?,?,?)";
	        PreparedStatement pstmt = SqliteManager.prepareStatement(sql);
			pstmt.setString	(1, uuid);
	        pstmt.setString	(2, fileParent);
			pstmt.setString	(3, f.getName());
			pstmt.setLong  	(4, f.lastModified());
			pstmt.setLong	(5, f.length());
			pstmt.setLong	(6, f.lastModified());
			pstmt.setLong	(7, f.hashCode());
			pstmt.execute();
			StateControl.incrementInventoriedFileCount();
			return 1;
		}
		catch(Exception e) {
			if (e.getMessage().startsWith("[SQLITE_CONSTRAINT]")) {
				Log.log(f.getAbsolutePath() + " is already in the inventory.");
			} else {
				Log.logException(e);
			}		
			return 0;
		}
	}
}
