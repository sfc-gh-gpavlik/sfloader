/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.sfloader;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import com.snowflake.community.jdbc.SnowflakeConnection;
import com.snowflake.community.utils.Log;

public class FileStager extends Thread {

	private static final int MAX_TRIES = 3;
	
	private SnowflakeConnection sfConnector;
	private Connection sfConn;
	private String uuid;
	private int threadID;
	
	public FileStager(int id) {
		threadID = id;
	}

	public void run() {
		try {
			init();
			StagedFile f;
			do{
				if(FileLoader.isStopped()) {
					Log.log("Staging thread " + threadID + " terminated after getting stop signal.");
					break;
				}
				f = claimFile();
				if (f != null) putFile(f);
			} while (f != null);
			Log.log("Staging thread " + threadID + " found no more files to stage in the inventory.");
		} catch (IllegalArgumentException | SQLException | IOException e) {
			Log.logException(e);
		}
	}
	
	private boolean putFile(StagedFile f) {		
		String sql = StateControl.getPutStatement();
		sql =  sql.replaceAll("(?i)<path_to_file>", f.getFilePath().replace("'", "''"));
		sql =  sql.replaceAll("(?i)<filename>", f.getFileName().replace("'", "''"));
		
		Log.log("Thread " + threadID + " putting file " + f.getFilePath() + File.separator + f.getFileName());
		
		Statement stmt;
		try {
			stmt = sfConn.createStatement();
		} catch (SQLException e) {
			Log.logException(e);
			return false;
		}

		try {
			ResultSet rs = stmt.executeQuery(sql);
			Log.log("Thread " + threadID + " successfully put file " + f.getFilePath() + File.separator + f.getFileName());
			
			MarkPutStatus(f, rs, true, "");
			FileLoader.incrementFilesStaged();
			FileLoader.addBytesStaged(rs.getLong("source_size"));
		} catch (SQLException e) {
			String msg = e.getMessage().replaceAll("[\\t\\n\\r]+"," ");
			Log.log(msg);
			MarkPutStatus(f, null, false, msg);
			return false;
		}
		return true;
	}

	private void MarkPutStatus(StagedFile f, ResultSet rs, boolean isSuccess, String errorMessage) {
		String status;
		String lastError;
		try {
			if(isSuccess) {
				if(!rs.next()) {
					status = "FAILED";
					lastError = "No result returned from PUT command.";
				} else {
					status = rs.getString("status");
					lastError = null; 
				}
			} else {
				status = "FAILED";
				lastError = errorMessage;
			}
			String sql =   "update FILE_INGESTION_CONTROL \n"
				         + "set    STAGING_STATUS = ?, "
				         + "       STAGING_LAST_END_TIME = ?, \n"
				         + "       STAGING_LAST_ERROR_MESSAGE = ?, \n"
				         + "       STAGING_OWNER_ID = null \n"
				         + "where  FILE_ID = ?;";
			
			PreparedStatement pstmt = SqliteManager.prepareStatement(sql);
			pstmt.setString(1, status);
			pstmt.setLong  (2, System.currentTimeMillis());
			pstmt.setString(3, lastError);
			pstmt.setString(4, f.getFileID());
			pstmt.execute();
		}
		catch(Exception e) {
			Log.logException(e);
		}		
	}

	private StagedFile claimFile() throws SQLException {
		
		String sql =  "update  FILE_INGESTION_CONTROL \n"
				    + "set     STAGING_STATUS          = 'STAGING', \n"
			        + "        STAGING_OWNER_ID        = ?, \n"
			        + "        STAGING_TRY_COUNT       = STAGING_TRY_COUNT + 1, \n"
			        + "        STAGING_LAST_START_TIME = ?, \n"
			        + "        STAGING_LAST_END_TIME   = null \n"
			        + "where   FILE_ID in \n"
			        + "    ( \n"
			        + "        select    FILE_ID \n"
			        + "        from      FILE_INGESTION_CONTROL \n"
			        + "        where     ( \n"
			        + "					   STAGING_OWNER_ID is null and \n"
			        + "					   (STAGING_STATUS not in ('SKIPPED', 'UPLOADED') or STAGING_STATUS is null) \n"	
			        + "                  ) \n"
			        + "				and  STAGING_TRY_COUNT < ? \n"
			        + "        order by  %s %s \n"
			        + "        limit 1 \n" 
			        + "    );";
		
		sql = String.format(sql, "FILE_INGESTION_ORDER", "ASC");
		
		PreparedStatement pstmt = SqliteManager.prepareStatement(sql);
		pstmt.setString(1, uuid);
		pstmt.setLong(2, System.currentTimeMillis());
		pstmt.setLong(3, MAX_TRIES); 
		
		SqliteManager.execute(pstmt);

		sql = "select FILE_ID, FILE_PROTOCOL, FILE_PATH, FILE_NAME, FILE_BYTES, FILE_LAST_MODIFIED, FILE_HASHCODE \n" +
		      "from   FILE_INGESTION_CONTROL \n" +
		      "where  STAGING_STATUS = 'STAGING' and STAGING_OWNER_ID = ? \n" +
		      "limit  1;";
		
		pstmt = SqliteManager.prepareStatement(sql);
		pstmt.setString(1, uuid);	
		ResultSet rs = pstmt.executeQuery();
		if (rs.next()) {
			StagedFile f = new StagedFile();
			f.setFileBytes(rs.getLong("FILE_BYTES"));
			f.setFileHashcode(rs.getLong("FILE_HASHCODE"));
			f.setFileID(rs.getString("FILE_ID"));
			f.setFileLastModified(rs.getLong("FILE_LAST_MODIFIED"));
			f.setFileName(rs.getString("FILE_NAME"));
			f.setFilePath(rs.getString("FILE_PATH"));
			f.setFileProtocol(rs.getString("FILE_PROTOCOL"));
			return f;
		} else {
			return null;
		}
	}
	
	private void init() throws IllegalArgumentException, SQLException, IOException {
		uuid = UUID.randomUUID().toString();
		sfConnector = new SnowflakeConnection();
		sfConnector.setAccountURL(StateControl.getSnowflakeAccount());		
		sfConnector.setUser(StateControl.getSnowflakeLogin());
		sfConnector.setPassword(StateControl.getSnowflakePassword());
		
		sfConn = sfConnector.getConnection();	
	}	
}
