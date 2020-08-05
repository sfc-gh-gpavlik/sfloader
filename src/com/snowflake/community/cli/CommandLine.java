/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Comparator;

import com.snowflake.community.sfloader.ActivityControl;
import com.snowflake.community.sfloader.FileLoader;
import com.snowflake.community.sfloader.PutStatementGenerator;
import com.snowflake.community.sfloader.SqliteManager;
import com.snowflake.community.sfloader.StateControl;
import com.snowflake.community.utils.AsciiArt;
import com.snowflake.community.utils.ExceptionHandler;
import com.snowflake.community.utils.Log;
import com.snowflake.community.utils.Reflection;
import com.snowflake.community.utils.StringUtils;
import com.snowflake.community.utils.UrlParser;
import com.snowflake.community.utils.clitable.AsciiTable;
import com.snowflake.community.utils.clitable.AsciiTableHeader;

public class CommandLine {
		
	private static final int LONGEST_LINE_LENGTH = 100;
	private static final String MASKED_PASSWORD = "********";
	
	private static final int KEY = 0;
	private static final int VALUE = 1;

	private static boolean isExit = false;	
	
	@SuppressWarnings("unused")
	private static int filesToStage = 0;		// Reserved for future implementation
	@SuppressWarnings("unused")
	private static long bytesToStage = 0;		// Reserved for future implementation
	
	private static BufferedReader reader;
	
	/**
	 * Runs an instance of the sfloader Command Line Interpreter (CLI)
	 * @param args Takes only one argument, the runtime mode. If left blank will default to GUI mode if available.
	 */
	public static void runCLI(String[] args) {
		
		try {
			reader = new BufferedReader(new InputStreamReader(System.in)); 
			String command = "";
			print("");
			do {
				System.out.print("sfloader>");
				command = reader.readLine();
				ProcessCommand(command);
			} while (!isExit);   
		} catch (IOException e) {			
			Log.logException(e);
		} 		
	}
	
	/**
	 * Runs the application in run mode, which stages all files in inventory and exits.
	 * @param args arguments passed through from the command line. Not used.
	 */
	public static void runRunMode(String[] args) {
		try {
			run();
			Log.log("Exiting from run mode.");
			System.exit(0);
		} catch (InterruptedException e) {
			Log.logException(e);
		}
		return;
	}
	
	/**
	 * Runs the application in auto mode, which inventories new files, stages them, and exits.
	 * @param args arguments passed through from the command line. Not used.
	 */
	public static void runAutoMode(String[] args) {
		try {
			inventory();
			run();
			Log.log("Exiting from auto mode.");
			System.exit(0);
		} catch (InterruptedException e) {
			Log.logException(e);
		}
		return;
	}
	
	/**
	 * Sends a message to stdout, for now use a simple System.out.println, but this may change.
	 * @param message
	 */
	private static void print(String message) {
		System.out.println(message);
	}

	private static void ProcessCommand(String command) {
	
		String cmdClean = command.replaceAll("\\s+", " ").trim();
		String cmdCompare = cmdClean.toLowerCase();	
		
		if (cmdCompare.compareTo("generate put") == 0) {
			String status = StateControl.readyForPutGeneration();
			if (status.compareTo("Ready") == 0) {
				generatePutStatement();
				print(StateControl.getPutStatement());
			} else {
				print(status);
			}
			return;
		}
		
		if (cmdCompare.compareToIgnoreCase("help") == 0 || cmdCompare.startsWith("help ")) {
			help();
			return;
		}
		
		if (cmdCompare.compareTo("snowflake") == 0) {
				print(AsciiArt.getAsciiSnowflake());
				return;
			}
		
		if (cmdCompare.compareTo("quit") == 0 || 
       		cmdCompare.compareTo("exit") == 0) {
			isExit = true;
			return;
		}
		
		if (cmdCompare.compareTo("inventory") == 0) {
			try {
				inventory();
			} catch (InterruptedException e) {
				Log.logException(e);
			}
			return;
		}
		
		if (cmdCompare.compareTo("run") == 0) {
			try {
				run();
			} catch (InterruptedException e) {
				Log.logException(e);
			}
			return;
		}
		
		if (cmdCompare.startsWith("set ")) {
			setProperty(command, cmdClean);
			return;
		}
		
		if (cmdCompare.startsWith("get ")) {
			print(getProperty(cmdClean.substring(3)));
			return;
		}
		
		if (cmdCompare.compareTo("get") == 0) {
			getAllProperties();
			return;
		}
		
		if (cmdCompare.compareTo("cls") == 0) {
			Log.clearConsole();
			return;
		}
		
		if (cmdCompare.compareTo("set") == 0) {
			setAllProperties();
			return;
		}
		
		if (cmdCompare.compareTo("clear") == 0) {
			clearInventory();
			return;
		}
		
		if (cmdCompare.startsWith("check ")) {
			check(command, cmdClean);
			return;
		}
		
		print("Invalid command: " + command);
		
	}
	
	private static void check(String command, String clean) {
		String[] words = clean.split(" ");
		String chk = words[1].trim().toLowerCase();
		if (chk.compareTo("run") == 0) {
			print(StateControl.readyToLoad());
		} 
		else if (chk.compareTo("put") == 0) {
			print(StateControl.readyForPutGeneration());
		} 
		else if (chk.compareTo("inventory") == 0) {
			print(StateControl.readyToInventoryFiles());
		}
		else if (chk.compareTo("connect") == 0) {
			print(StateControl.readyToConnectToSnowflake());
		} else {
			print("Unknown check activity: " + clean);
		}
	}
	
	private static void clearInventory() {
		print("Are you sure you want to clear the file inventory (y/n)?");
		String confirm;
		try {
			confirm = reader.readLine();
			if (confirm.trim().toLowerCase().startsWith("y")) {
				ActivityControl.DeleteFileInventory();
				print("File inventory cleared.");
			} else {
				print("Cancelled.");
			}
		} catch (Exception e) {
			Log.logException(e);
		}
	}
	
	private static void setProperty(String command, String clean) {
		
		final int PROPERTY = 1;
		final int VALUE = 2;
		
		boolean foundProperty = false;

		Object[] args = new Object[1];
		String displayValue = "";
		
		String[] words = clean.split(" ");
		
		for (Method m : Reflection.getMethods(StateControl.class)) {
			if(m.getName().toLowerCase().compareTo("set" + words[PROPERTY].toLowerCase()) == 0) {
				try {
					if (m.getName().toLowerCase().contains("password")) {
						args[0] = collectPassword("Password: ");
						m.invoke(StateControl.class, args);
						displayValue = MASKED_PASSWORD;
					} else if (m.getName().compareTo("setSnowflakeAccount") == 0) {
						args[0] = UrlParser.SnowflakeAccountFromURL(words[VALUE]);
						m.invoke(StateControl.class, args);
						displayValue = args[0].toString();	
					} else {
						args[0] = words[VALUE];
						m.invoke(StateControl.class, args);
						displayValue = words[VALUE];						
					}
					print(m.getName().substring(3) + " set to " + displayValue);	
				} 
				catch (ArrayIndexOutOfBoundsException e) {
					print("Missing value to set the property. Syntax:  set <property_name> <property_value>");
				} 
				catch (InvocationTargetException e) {
					print ("Cannot set to value: " + ExceptionHandler.getRootCauseMessage(e));
				} 
				catch (Exception e) {
					Log.logException(e);
				}
				foundProperty = true;
				break;
			}
		}
		if (!foundProperty) {
			print("Property " + words[PROPERTY] + " not found or is not a settable property.");
		}	
	}
	
	private static void setAllProperties() {
		ArrayList<String> settable = new ArrayList<String>();		
		String value = null;
		for (Method m : Reflection.getMethods(StateControl.class)) {
			if(m.getName().startsWith("set")) {
				try {
					settable.add(m.getName().substring(3));
				} catch (Exception e) {
					Log.logException(e);
				}
			}
		}
		
		Collections.sort(settable);
		String[][] data = new String[settable.size()][2];
		for(int i = 0; i < settable.size(); i++) {
			data[i][0] = settable.get(i);
			value = getProperty(data[i][0]);
			if(settable.get(i).length() > LONGEST_LINE_LENGTH || value.contains("\n")) {
				data[i][1] = "[ Long or multi-line text. Get setting individually. ]";
			} else {
				data[i][1] = value;
			}
		}
		AsciiTableHeader[] header = {new AsciiTableHeader("Settable Property", AsciiTable.ALIGN_LEFT),
									 new AsciiTableHeader("Current Value", AsciiTable.ALIGN_LEFT)};
		AsciiTable.printTable(header, data);	
	}
	
	private static String getProperty(String propertyName) {
		
		String pName = "get" + propertyName.toLowerCase().trim();
		Object[] args = null;
		String value;
		
		for (Method m : Reflection.getMethods(StateControl.class)) {			
			if(pName.compareTo(m.getName().toLowerCase()) == 0) {
				if(pName.contains("password")){
					try {
						value = StringUtils.getNullSafeString(m.invoke(StateControl.class, args));
						if(value.compareTo("") == 0) {
							return "";
						} else {
							return MASKED_PASSWORD;
						}
					} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
						Log.logException(e);
					}
					return "****";
				} else {
					try {
						return StringUtils.getNullSafeString(m.invoke(StateControl.class, args));
					} catch (Exception e) {
						Log.logException(e);
					}
				}
			}
		}
		return "";
	}
	
	private static String collectPassword(String promptText) {
		char[] password = System.console().readPassword(promptText);
		return new String(password);
	}
	
	private static void inventory() throws InterruptedException {
		
		String status = StateControl.readyToInventoryFiles();
		
		if (status.compareTo("Ready") != 0) {
			print(status);
			return;
		}
		
		ActivityControl.inventoryFiles();
		do {
			Thread.sleep(1000);
		} while(ActivityControl.isInventoryRunning());
		Log.log("Inventory done.");
		
	}
	
	private static void generatePutStatement() {
		PutStatementGenerator put = new PutStatementGenerator();
		put.setDatabase(StateControl.getDatabase());
		put.setSchema(StateControl.getSchema());
		put.setStage(StateControl.getStage());
		put.setOverwrite(StateControl.getOverwriteExisting());
		put.setSourceCompression(StateControl.getSourceCompression());
		StateControl.setPutStatement(put.generate());
	}
	
	private static void run() throws InterruptedException {
		
		String status = StateControl.readyToLoad();
		
		if (status.compareTo("Ready") != 0) {
			print(status);
			return;
		}
		
		try {
			Connection conn = SqliteManager.getConnection();
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("select * from GET_STAGE_INFO;");
			filesToStage = rs.getInt("FILES_TO_STAGE");
			bytesToStage = rs.getLong("BYTES_TO_STAGE");
		} catch (SQLException e) {
			Log.logException(e);
		}
		FileLoader.loadFiles(StateControl.getThreadCount());
		do {
			Thread.sleep(1000);
		} while(FileLoader.isRunning());
		Log.log("All file staging threads have terminated.");
	}
	
	private static void getAllProperties() {
	
		ArrayList<String[]> table = new ArrayList<String[]>();
		String[] property;
		//Object o = null;
		//Object[] args = null;	
		
		for (Method m : Reflection.getMethods(StateControl.class)) {
			if(m.getName().startsWith("get")) {
				try {
					property = new String[2];
					property[KEY] = m.getName().substring(3);
					property[VALUE] = getProperty(property[KEY]);
					table.add(property);
				} catch (Exception e) {
					Log.logException(e);
				}
			}
		}
		
        Collections.sort(table,new Comparator<String[]>() {
            public int compare(String[] strings, String[] otherStrings) {
                return strings[0].compareTo(otherStrings[0]);
            }
        });
		
		buildTable(table);
	}
	
	private static void buildTable(ArrayList<String[]> table) {
		
		AsciiTableHeader [] header = {
			      new AsciiTableHeader("Property", AsciiTable.ALIGN_LEFT), 
			      new AsciiTableHeader("Value", AsciiTable.ALIGN_LEFT)
			};
		
		String[][] data = new String[table.size()][2];
		
		for (int i = 0; i < table.size(); i++) {
			data[i][KEY] = table.get(i)[KEY];
			if(table.get(i)[VALUE].length() > LONGEST_LINE_LENGTH || table.get(i)[VALUE].contains("\n")) {
				data[i][VALUE] = "[ Long or multi-line text. Get setting individually. ]";
			} else {
				data[i][VALUE] = table.get(i)[VALUE];	
			}
		}
		AsciiTable.printTable(header, data);
	}
	
	private static void help() {
		AsciiTableHeader[] header = {
			      new AsciiTableHeader("Command", 		AsciiTable.ALIGN_LEFT, 	AsciiTable.ALIGN_CENTER), 
			      new AsciiTableHeader("Parameter(s)", 	AsciiTable.ALIGN_LEFT, 	AsciiTable.ALIGN_CENTER),
			      new AsciiTableHeader("Description", 	AsciiTable.ALIGN_LEFT, 	AsciiTable.ALIGN_CENTER)};
		
		String[][] data = 
			{
				{"check", 		"connect",					"Checks the connection to Snowflake."},
				{"check", 		"inventory",				"Checks if the app is ready to inventory files."},
				{"check", 		"put",						"Checks if the app is ready to generate the put statement."},
				{"check", 		"run",						"Checks if the app is ready to run the job to put files."},
				{"clear", 		"",							"Clears the inventory of local files."}, 
				{"cls", 		"",							"Clears the screen."}, 
				{"generate", 	"put", 						"Generates the put statement template to use."}, 
				{"get", 		"", 						"Gets a list of all properties."},
				{"get", 		"<property_name>", 			"Generates the put statement template to use."},
				{"inventory", 	"", 						"Inventories local files to the database."},
				{"set", 		"", 						"Lists all settable properties."},
				{"set", 		"<property_name> <value>", 	"Sets a property. (Do not use for password.)"},
				{"set", 		"SnowflakePassword", 		"Sets the Snowflake password without showing it."},
				{"set", 		"", 						"Lists all settable properties."},
				{"run", 		"", 						"Runs the job to stage all inventoried files."}
			};
		AsciiTable.printTable(header, data);
	}	
}

