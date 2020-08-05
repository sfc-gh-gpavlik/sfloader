/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.sfloader;

import java.awt.GraphicsEnvironment;

import com.snowflake.community.cli.CommandLine;
import com.snowflake.community.sfloader.ui.MainForm;
import com.snowflake.community.utils.Log;

public class Main {
	
	private static final int EXIT_CODE_UNKNOWN_PARAMETER = 1;
	
	private static final int GUI_MODE  = 0;
	private static final int CLI_MODE  = 1;
	private static final int RUN_MODE  = 2;
	private static final int AUTO_MODE = 3;
	
	/**
	 * The main entry point for the application. Invokes a GUI or CLI instance depending on environment and
	 * command line arguments.
	 * @param args the command line arguments. Uses only the first argument.
	 */
	public static void main(String[] args) {
		
		Log.clearConsole();
		
		// Assume GUI mode unless mode is set by command line parameter.
		int mode = GUI_MODE;	
		
		// Print the abbreviated license.
		System.out.println(License.getLicense());
		
		// Force CLI mode if no command line parameter and no UI available.
		if (GraphicsEnvironment.isHeadless()) {
			mode = CLI_MODE;
		} else {
			mode = GUI_MODE;
		}
		
		// Switch mode if explicitly set in command line.
		for(int i=0; i <args.length; i++) {
			String arg1 = args[i].trim().toLowerCase();
			if (arg1.compareTo("cli") == 0) {
				mode = CLI_MODE;
			} 
			else if (arg1.compareTo("run") == 0) {
				mode = RUN_MODE;
			} 
			else if (arg1.compareTo("auto") == 0) {
				mode = AUTO_MODE;				
			} else { 
				Log.log("Exiting due to unknown command line parameter: " + args[i] + "\n" +
					    "Application takes 0 or 1 parameters. Options are CLI for command line mode, RUN for run mode, and AUTO for auto mode."); 
				System.exit(EXIT_CODE_UNKNOWN_PARAMETER);
			}
		}

		SqliteManager.initialize();
		StatePersistence.loadStateFromDatabase();

		switch (mode) {
			case GUI_MODE:
				Log.out("Running in GUI mode.");
				MainForm.main(args);
				break;
			case CLI_MODE:
				Log.out("Running in command line mode.");
				CommandLine.runCLI(args);
				break;
			case RUN_MODE:
				Log.out("Running in run mode.");
				CommandLine.runRunMode(args);
				break;
			case AUTO_MODE:
				Log.log("Running in auto mode.");
				CommandLine.runAutoMode(args);
				break;
		}
	}
}
