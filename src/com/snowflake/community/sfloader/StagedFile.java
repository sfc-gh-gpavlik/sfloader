/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.sfloader;

public class StagedFile {

    private String fileID;
    private String fileProtocol;
    private String filePath;
    private String fileName;
    private long   fileBytes;
    private long   fileLastModified;
    private long   fileHashcode;
    
	public String getFileProtocol() {
		return fileProtocol;
	}
	public void setFileProtocol(String fileProtocol) {
		this.fileProtocol = fileProtocol;
	}
	public String getFileID() {
		return fileID;
	}
	public void setFileID(String fileID) {
		this.fileID = fileID;
	}
	public String getFilePath() {
		return filePath;
	}
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	public long getFileBytes() {
		return fileBytes;
	}
	public void setFileBytes(long fileBytes) {
		this.fileBytes = fileBytes;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public long getFileLastModified() {
		return fileLastModified;
	}
	public void setFileLastModified(long fileLastModified) {
		this.fileLastModified = fileLastModified;
	}
	public long getFileHashcode() {
		return fileHashcode;
	}
	public void setFileHashcode(long fileHashcode) {
		this.fileHashcode = fileHashcode;
	}
}
