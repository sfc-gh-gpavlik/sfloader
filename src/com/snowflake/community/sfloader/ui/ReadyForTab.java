/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.sfloader.ui;

import com.snowflake.community.sfloader.StateControl;

public class ReadyForTab {

	public static final String READY = "Ready";
	
	public static String readyForPutTab() {
		if (!StateControl.isConnected()) return "You must connect to Snowflake before moving to the PUT statement tab.";
		if (StateControl.getInventoriedFileCount() == 0) return "You must inventory files before putting the files to a Snowflake stage.";
		if (StateControl.getDatabase() == null || StateControl.getDatabase().equals("")) return "You must select a database before generating the put statement.";
		if (StateControl.getSchema() == null || StateControl.getSchema().equals("")) return "You must select a schema before generating the put statement.";
		if (StateControl.getStage() == null || StateControl.getStage().equals("")) return "You must select a stage before generating the put statement.";
		if (StateControl.getSnowflakeAccount() == null || StateControl.getSnowflakeAccount().equals("")) return "You must set the Snowflake account before generating the put statement.";
		if (StateControl.getSnowflakeLogin() == null || StateControl.getSnowflakeLogin().equals("")) return "You must set the Snowflake login before generating the put statement.";
		if (StateControl.getSnowflakePassword() == null || StateControl.getSnowflakePassword().equals("")) return "You must set the Snowflake password before generating the put statement.";
		return READY;
	}
	

}
