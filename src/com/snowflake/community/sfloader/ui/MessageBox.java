/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.sfloader.ui;

import javax.swing.JFrame;
import javax.swing.JOptionPane;

public class MessageBox {
	public static void show(JFrame frame, String message, String title) {
		JOptionPane.showMessageDialog(frame, message, title, JOptionPane.INFORMATION_MESSAGE);
	}	
}