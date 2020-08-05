/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.sfloader;

public class PutStatementGenerator implements SqlGenerator {

	private String database;
	private String schema;
	private String stage;
	private String sourceCompression;
	private boolean overwrite;
	private int parallel = 4;  // Default number of chunks JDBC driver will use to split large files.
		
	@Override
	public String generate() {
		String sql  = "put 'file://<path_to_file>/<filename>' @\"" + database + "\".\"" + schema + "\".\"" + stage + "\"\n";
			   sql += "     source_compression = " + sourceCompression + "\n";
			   sql += "     auto_compress      = " + getAutoCompress() + "\n";
			   sql += "     overwrite          = " + overwrite + "\n";
			   sql += "     parallel           = " + parallel;
		
		return sql;
	}

	public boolean getAutoCompress() {
		if (sourceCompression == null) return true;
		return (sourceCompression.trim().toUpperCase().equals("NONE")); 
	}
	public String getSourceCompression() {
		return sourceCompression;
	}
	public void setSourceCompression(String sourceCompression) {
		this.sourceCompression = sourceCompression;
	}
	public boolean getOverwrite() {
		return overwrite;
	}
	public void setOverwrite(boolean overwrite) {
		this.overwrite = overwrite;
	}
	public String getDatabase() {
		return database;
	}
	public void setDatabase(String database) {
		this.database = database;
	}
	public String getSchema() {
		return schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
	}
	public String getStage() {
		return stage;
	}
	public void setStage(String stage) {
		this.stage = stage;
	}
}