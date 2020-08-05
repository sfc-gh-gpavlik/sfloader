/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.sfloader.ui;

import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JTextField;
import javax.swing.JPasswordField;
import javax.swing.JPanel;
import javax.swing.JLabel;
import java.awt.GridLayout;

public class CredentialsBox {
	
	// Add basic validation checks before dismissing.
	
	private String user = "";
	private String password = "";
	
	public int getCredentials(JFrame frame, String prompt) {
		return showCredentialsDialog(frame, prompt);
	}
	
	public String getUser() {
		return user;
	}
	
	public String getPassword() {
		return password;
	}
	
	private int showCredentialsDialog(JFrame frame, String prompt)
	{
		JPanel snowflakeCredentialsPanel = new JPanel();
		snowflakeCredentialsPanel.setLayout(new GridLayout(4,4));
		JLabel lableUser = new JLabel("Username:");
		JLabel labelPassword = new JLabel("Password:");
		JTextField username = new JTextField();
		JPasswordField passwordFld = new JPasswordField();
		
		snowflakeCredentialsPanel.add(lableUser);
		snowflakeCredentialsPanel.add(username);
		snowflakeCredentialsPanel.add(labelPassword);
		snowflakeCredentialsPanel.add(passwordFld);
	
		int input = JOptionPane.showConfirmDialog(frame, snowflakeCredentialsPanel, prompt, 
				JOptionPane.OK_CANCEL_OPTION, JOptionPane.PLAIN_MESSAGE);
	
		if (input == 0) {
			char[] enteredPassword = passwordFld.getPassword();
			user = (username.getText());
			password = (new String(enteredPassword));
		}
		return input;
	}
}