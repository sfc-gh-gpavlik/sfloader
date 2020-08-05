/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.jdbc;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class SnowflakeObjectInfo {

	/**
	 * Returns an array filled with the database names in a Snowflake account
	 * @param conn A connected JDBC connection to the Snowflake account
	 * @return An array of database names in the account
	 * @throws IOException
	 */
	public static String[] getDatabases(SnowflakeConnection conn) throws IOException {		
		ArrayList<String> dbList = new ArrayList<String>();
		Statement stmt;
		try {
			stmt = conn.getConnection().createStatement();
			ResultSet rs = stmt.executeQuery("show databases;");
			while (rs.next()) {
				dbList.add(rs.getString("name"));
			}
			String[] db = dbList.toArray(new String[0]);
			return db;
		} catch (IllegalArgumentException | SQLException e) {
			e.printStackTrace();
			return new String[0];
		} catch (IOException e) {
			e.printStackTrace();
			return new String[0];
		}
	}
	
	/**
	 * Returns an array filled with the warehouse names in a Snowflake account
	 * @param conn A connected JDBC connection to the Snowflake account
	 * @return An array of warehouse names in the account
	 */
	public static String[] getWarehouses(SnowflakeConnection conn) {
		ArrayList<String> whList = new ArrayList<String>();
		Statement stmt;
		try {
			stmt = conn.getConnection().createStatement();
			ResultSet rs = stmt.executeQuery("show warehouses;");
			while (rs.next()) {
				whList.add(rs.getString("name") + " (" + 
						   rs.getString("size") + ")");
			}
			String[] wh = whList.toArray(new String[0]);
			return wh;
		} catch (IllegalArgumentException | SQLException e) {
			e.printStackTrace();
			return new String[0];
		} catch (IOException e) {
			e.printStackTrace();
			return new String[0];
		}	
	}
	
	/**
	 * Returns an array filled with all the schemas in a Snowflake database
	 * @param conn A connected JDBC connection to the Snowflake account
	 * @param database The database containing the schemas
	 * @return An array of schema names in the database
	 */
	public static String[] getSchemata(SnowflakeConnection conn, String database) {
		String sql = "show schemas in database \"" + database + "\"";
		ArrayList<String> dbList = new ArrayList<String>();
		Statement stmt;
		String name;
		try {
			stmt = conn.getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				name = rs.getString("name");
				if (!name.equals("INFORMATION_SCHEMA")) {
					dbList.add(rs.getString("name"));
				}
			}
			String[] db = dbList.toArray(new String[0]);
			return db;
		} catch (IllegalArgumentException | SQLException e) {
			e.printStackTrace();
			return new String[0];
		} catch (IOException e) {
			e.printStackTrace();
			return new String[0];
		}	
	}
	
	/**
	 * Returns an array filled with all the tables in a Snowflake schema
	 * @param conn A connected JDBC connection to the Snowflake account
	 * @param database The database containing the tables
	 * @param schema The schema containing the tables
	 * @return An array of table names in the schema
	 */
	public static String[] getTables(SnowflakeConnection conn, String database, String schema) {
		String sql = "show tables in schema \"" + database + "\".\"" + schema + "\"";
		ArrayList<String> dbList = new ArrayList<String>();
		Statement stmt;
		try {
			stmt = conn.getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				dbList.add(rs.getString("name"));
			}
			String[] db = dbList.toArray(new String[0]);
			return db;
		} catch (IllegalArgumentException | SQLException | IOException e) {
			e.printStackTrace();
			return new String[0];
		}	
	}
	
	/**
	 * Returns an array filled with file formats 
	 * @param conn A connected JDBC connection to the Snowflake account
	 * @param database The database containing the file formats
	 * @param schema The schema containing the file formats
	 * @return An array of file format names in the schema
	 */
	public static String[] getFileFormats(SnowflakeConnection conn, String database, String schema) {
		String sql = "show file formats in schema \"" + database + "\".\"" + schema + "\"";
		ArrayList<String> dbList = new ArrayList<String>();
		Statement stmt;
		try {
			stmt = conn.getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				dbList.add(rs.getString("name"));
			}
			String[] db = dbList.toArray(new String[0]);
			return db;
		} catch (IllegalArgumentException | SQLException | IOException e) {
			e.printStackTrace();
			return new String[0];
		}	
	}
	
	/**
	 * Returns an array filled with stage names
	 * @param conn A connected JDBC connection to the Snowflake account
	 * @param database The database containing the stages
	 * @param schema The schema containing the stages
	 * @return An array of stage names in the schema
	 */
	public static String[] getStages(SnowflakeConnection conn, String database, String schema) {
		String sql = "show stages in schema \"" + database + "\".\"" + schema + "\"";
		ArrayList<String> dbList = new ArrayList<String>();
		Statement stmt;
		try {
			stmt = conn.getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				dbList.add(rs.getString("name"));
			}
			String[] db = dbList.toArray(new String[0]);
			return db;
		} catch (IllegalArgumentException | SQLException | IOException e) {
			e.printStackTrace();
			return new String[0];
		}	
	}
}
