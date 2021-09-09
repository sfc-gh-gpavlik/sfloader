/*
 * Copyright (c) 2020, 2021 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.sfloader.ui;

import java.awt.EventQueue;
import javax.swing.JFrame;
import javax.swing.JTabbedPane;
import javax.swing.JTextField;
import javax.swing.Timer;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import com.snowflake.community.jdbc.SnowflakeConnection;
import com.snowflake.community.jdbc.SnowflakeObjectInfo;
import com.snowflake.community.sfloader.ActivityControl;
import com.snowflake.community.sfloader.FileInventory;
import com.snowflake.community.sfloader.FileLoader;
import com.snowflake.community.sfloader.PutStatementGenerator;
import com.snowflake.community.sfloader.SqliteManager;
import com.snowflake.community.sfloader.StateControl;
import com.snowflake.community.utils.Log;
import com.snowflake.community.utils.UrlParser;

import javax.swing.JPanel;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import java.awt.event.ActionListener;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.awt.event.ActionEvent;
import javax.swing.JPasswordField;
import javax.swing.JLabel;
import javax.swing.JOptionPane;

import java.awt.Toolkit;
import java.io.IOException;

import javax.swing.JCheckBox;
import javax.swing.JSeparator;
import java.awt.Color;
import java.awt.SystemColor;
import javax.swing.JComboBox;
import javax.swing.SwingConstants;
import javax.swing.DefaultComboBoxModel;
import java.awt.event.ItemListener;
import java.awt.event.ItemEvent;
import javax.swing.JTextArea;
import java.awt.Font;
import javax.swing.JScrollPane;
//import javax.swing.event.ChangeListener;
//import javax.swing.event.ChangeEvent;
import javax.swing.JProgressBar;
import javax.swing.event.ChangeListener;
import javax.swing.event.ChangeEvent;

public class MainForm {
	
	static final String RESOURCE_PATH = "/com/snowflake/community/resources";
	
	static final int FILES_PANE = 0;
	static final int SNOWFLAKE_PANE = 1;
	static final int PUT_PANE = 2;
	static final int LOAD_PANE = 3;

	SnowflakeConnection snowflakeConnection = new SnowflakeConnection();
	
	private static Timer fileInventoryTimer;
	private static Thread fileInventory;
	
	private static Timer fileStagerTimer;
	private static long bytesToStage; 
	
	private boolean isInitializing;
	
	private JFrame 				frame;
	private JTabbedPane 		tabs;
	private JTextField 			textLocalFileDirectory;
	private JTextField 			textSnowflakeAccount;
	private JPasswordField 		textSnowflakePassword;
	private JTextField 			textSnowflakeUser;
	private JLabel 				snowflakeConnectionStatus;
	private JLabel 				fileInventoryLabel;
	private JTextField 			textPositiveRegex;
	private JTextField 			textNegativeRegex;
	private JComboBox<String> 	sfDatabase;
	private JComboBox<String> 	sfSchema;
	private JComboBox<String> 	sfStage;
	private JComboBox<Integer>  comboBoxLoaderThreads;
	private JButton 			inventoryFiles;
	private JLabel 				filesInventory;
	private JComboBox<String> 	sourceCompressionComboBox;
	private JTextArea 			textPutStatement;
	
	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					MainForm window = new MainForm();
					window.frame.setVisible(true);
				} catch (Exception e) {
					Log.logException(e);;
				}
			}
		});
	}

	/**
	 * Create the application.
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws IllegalArgumentException 
	 */
	public MainForm() throws IllegalArgumentException, SQLException, IOException {
		isInitializing = true;		
		if(StateControl.getDoNotStoreCredentials()) {
			CredentialsBox cb = new CredentialsBox();
			int status = cb.getCredentials(frame, "Enter your Snowflake credentials:");
			if (status == 0) {
				StateControl.setSnowflakePassword(cb.getPassword());
				StateControl.setSnowflakeLogin(cb.getUser());
			}
		} // May want to test them before leaving here...
		
		
		initialize();
		
		if (StateControl.readyToConnectToSnowflake().equals("Ready")) {
			snowflakeConnection.setAccountURL(StateControl.getSnowflakeAccount());
			snowflakeConnection.setUser(StateControl.getSnowflakeLogin());
			snowflakeConnection.setPassword(StateControl.getSnowflakePassword());
			try {
				snowflakeConnection.getConnection();
			}
			catch(Exception e) {
				Log.logException(e);
			}
			StateControl.connectionStatus(snowflakeConnection.isConnected());
		}
		finalInit();
		isInitializing = false;
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		
		
		frame = new JFrame();
		frame.setIconImage(Toolkit.getDefaultToolkit().getImage(MainForm.class.getResource(RESOURCE_PATH + "/images/snowflake.png")));
		frame.setBounds(100, 100, 1073, 588);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.getContentPane().setLayout(null);
		
		JTabbedPane tabbedPane = new JTabbedPane(JTabbedPane.TOP);
		tabs = tabbedPane;
		tabbedPane.setBounds(6, 6, 1067, 560);
		frame.getContentPane().add(tabbedPane);
			
		JPanel panel_Files = new JPanel();
		tabbedPane.addTab("Inventory Files", null, panel_Files, null);
		tabbedPane.setDisabledIconAt(0, new ImageIcon(MainForm.class.getResource(RESOURCE_PATH + "/images/lock.png")));
		panel_Files.setLayout(null);
			
		textLocalFileDirectory = new JTextField();
		textLocalFileDirectory.setText(StateControl.getLocalDirectory());
		textLocalFileDirectory.setBackground(Color.WHITE);
		textLocalFileDirectory.setBounds(6, 95, 1034, 26);
		panel_Files.add(textLocalFileDirectory);
		textLocalFileDirectory.setColumns(10);
			
		textLocalFileDirectory.getDocument().addDocumentListener(new DocumentListener() {	
			public void changedUpdate(DocumentEvent e) {
				persist();
			}
			public void removeUpdate(DocumentEvent e) {
				persist();
			}
			public void insertUpdate(DocumentEvent e) {
				persist();
			}
			public void persist() {
				StateControl.setLocalDirectory(textLocalFileDirectory.getText());
				StateControl.connectionStatus(false);
				snowflakeConnectionStatus.setText("Not connected");
			}
		});
			
		JButton buttonChooseDirectory = new JButton("Choose Directory");
		buttonChooseDirectory.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				final JFileChooser fc = new JFileChooser();
				fc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
				int returnVal = fc.showOpenDialog(null);	
		        if (returnVal == JFileChooser.APPROVE_OPTION) {
		            textLocalFileDirectory.setText(fc.getSelectedFile().getAbsolutePath());
		        } 
			}
		});
		buttonChooseDirectory.setBounds(6, 65, 163, 29);
		panel_Files.add(buttonChooseDirectory);
		
		JButton buttonInventoryFiles = new JButton("Inventory Files");
		buttonInventoryFiles.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				String state = StateControl.readyToInventoryFiles();
				if (state.equals("Ready")) {
	        		fileInventoryLabel.setText("Collecting inventory...");
	        		buttonInventoryFiles.setEnabled(false);
	        		fileInventoryTimer.start();
					fileInventory = new FileInventory();		// Change this to use ActivityControl
					fileInventory.start();
				} else {
					JOptionPane.showMessageDialog(null, state);
				}
			
			}
		});
		buttonInventoryFiles.setBounds(6, 398, 163, 29);
		panel_Files.add(buttonInventoryFiles);
		
		JLabel lblFileInventory = new JLabel("Counting inventoried files...");
		lblFileInventory.setBounds(181, 403, 326, 16);
		panel_Files.add(lblFileInventory);
		
		JCheckBox checkboxTraverseSubDirs = new JCheckBox("Inventory files in subdirectories");
		checkboxTraverseSubDirs.addChangeListener(new ChangeListener() {
			public void stateChanged(ChangeEvent arg0) {
			
	
			}
		});
		checkboxTraverseSubDirs.setSelected(true);
		checkboxTraverseSubDirs.setBounds(6, 121, 691, 23);
		panel_Files.add(checkboxTraverseSubDirs);
		//localFileDirectory = textLocalFileDirectory;
		fileInventoryLabel = lblFileInventory;
		
		JButton btnClearFileInventory = new JButton("Clear File Inventory");
		btnClearFileInventory.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
			
				int dialogResult = JOptionPane.showConfirmDialog(frame, 
							"This will clear all inventoried files. Are you sure?", 
							"Confirm",
							JOptionPane.YES_NO_OPTION,
							JOptionPane.QUESTION_MESSAGE);
				if(dialogResult == JOptionPane.YES_OPTION) {
					try {
						ActivityControl.DeleteFileInventory();

						tabs.setEnabledAt(SNOWFLAKE_PANE, false);
						tabs.setEnabledAt(LOAD_PANE, false);
						tabs.setEnabledAt(PUT_PANE, false);
						
					} catch (SQLException e1) {
						Log.logException(e1);
					}
					filesInventory.setText("Inventory has " + StateControl.getInventoriedFileCount() + " files.");
				} 
			}
		});
		btnClearFileInventory.setBounds(6, 464, 163, 29);
		panel_Files.add(btnClearFileInventory);
		
		textPositiveRegex = new JTextField();
		textPositiveRegex.setForeground(Color.BLACK);
		textPositiveRegex.setFont(new Font("Courier New", Font.BOLD, 13));
		textPositiveRegex.setText(StateControl.getPositiveRegex());
		textPositiveRegex.setColumns(10);
		textPositiveRegex.setBounds(6, 246, 1034, 26);
		panel_Files.add(textPositiveRegex);
		textPositiveRegex.getDocument().addDocumentListener(new DocumentListener() {	
			  public void changedUpdate(DocumentEvent e) {
			    persist();
			  }
			  public void removeUpdate(DocumentEvent e) {
			    persist();
			  }
			  public void insertUpdate(DocumentEvent e) {
			    persist();
			  }
			  public void persist() {
				 StateControl.setPositiveRegex(textPositiveRegex.getText());
			  }
			});
		
		textNegativeRegex = new JTextField();
		textNegativeRegex.setFont(new Font("Courier New", Font.BOLD, 13));
		textNegativeRegex.setText(StateControl.getNegativeRegex());
		textNegativeRegex.setColumns(10);
		textNegativeRegex.setBounds(6, 308, 1034, 26);
		panel_Files.add(textNegativeRegex);
		textNegativeRegex.getDocument().addDocumentListener(new DocumentListener() {	
			  public void changedUpdate(DocumentEvent e) {
			    persist();
			  }
			  public void removeUpdate(DocumentEvent e) {
			    persist();
			  }
			  public void insertUpdate(DocumentEvent e) {
			    persist();
			  }
			  public void persist() {
				 StateControl.setNegativeRegex(textNegativeRegex.getText());
			  }
			});
		
		JLabel lblNewLabel_3 = new JLabel("File Name Regular Expression (optional):");
		lblNewLabel_3.setBounds(21, 230, 450, 16);
		panel_Files.add(lblNewLabel_3);
		
		JLabel lblNewLabel_4 = new JLabel("File Name Negative Regular Expression (optional):");
		lblNewLabel_4.setBounds(21, 291, 450, 16);
		panel_Files.add(lblNewLabel_4);
		
		JLabel lblNewLabel_3_1 = new JLabel("Local Directory for Files:");
		lblNewLabel_3_1.setBounds(181, 70, 181, 16);
		panel_Files.add(lblNewLabel_3_1);
		
		JButton btnUnlockSnowflake = new JButton("Next >>");
		btnUnlockSnowflake.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				if (StateControl.getInventoriedFileCount() > 0) {
					tabs.setEnabledAt(SNOWFLAKE_PANE, true);
					tabbedPane.setSelectedIndex(SNOWFLAKE_PANE);
				} else {
					tabs.setEnabledAt(SNOWFLAKE_PANE, false);
					MessageBox.show(frame, "You must inventory files before moving to the next tab.",
							"Not Ready to Advance");
				}
			}
		});
		btnUnlockSnowflake.setBounds(905, 479, 135, 29);
		panel_Files.add(btnUnlockSnowflake);
		
		JSeparator separator = new JSeparator();
		separator.setForeground(SystemColor.inactiveCaptionText);
		separator.setBounds(6, 156, 1034, 12);
		panel_Files.add(separator);
		
		JSeparator separator_1 = new JSeparator();
		separator_1.setForeground(Color.GRAY);
		separator_1.setBounds(6, 346, 1034, 12);
		panel_Files.add(separator_1);
		
		JLabel lblNewLabel_6_1 = new JLabel("Directory Containing Files to Load:");
		lblNewLabel_6_1.setFont(new Font("Lucida Grande", Font.PLAIN, 16));
		lblNewLabel_6_1.setBounds(20, 10, 576, 32);
		panel_Files.add(lblNewLabel_6_1);
			
		JPanel panel_Snowflake = new JPanel();
		tabbedPane.addTab("Connect to Snowflake", null, panel_Snowflake, null);
		tabbedPane.setEnabledAt(1, true);
		tabs.setDisabledIconAt(1, new ImageIcon(MainForm.class.getResource(RESOURCE_PATH + "/images/lock.png")));
		tabs.setEnabledAt(1, false);
		panel_Snowflake.setLayout(null);
				
		textSnowflakeAccount = new JTextField();
		textSnowflakeAccount.setText(StateControl.getSnowflakeAccount());
		textSnowflakeAccount.setBounds(101, 86, 939, 26);
		panel_Snowflake.add(textSnowflakeAccount);
		textSnowflakeAccount.setColumns(10);
				
		textSnowflakeAccount.getDocument().addDocumentListener(new DocumentListener() {	
			public void changedUpdate(DocumentEvent e) {
				persist();
			}
			public void removeUpdate(DocumentEvent e) {
				persist();
			}
			public void insertUpdate(DocumentEvent e) {
				persist();
			}
			public void persist() {
				StateControl.setSnowflakeAccount(textSnowflakeAccount.getText());
				snowflakeConnectionStatus.setText("Not connected");
				snowflakeConnection.setAccountURL(textSnowflakeAccount.getText());
			}
		});
				
		textSnowflakeUser = new JTextField();
		textSnowflakeUser.setText(StateControl.getSnowflakeLogin());
		textSnowflakeUser.setBounds(101, 162, 939, 26);
		panel_Snowflake.add(textSnowflakeUser);
		textSnowflakeUser.setColumns(10);
				
		textSnowflakeUser.getDocument().addDocumentListener(new DocumentListener() {	
			public void changedUpdate(DocumentEvent e) {
				persist();
			}
			public void removeUpdate(DocumentEvent e) {
				persist();
			}
			public void insertUpdate(DocumentEvent e) {
				persist();
			}
			public void persist() {
				StateControl.setSnowflakeLogin(textSnowflakeUser.getText());
				snowflakeConnectionStatus.setText("Not connected");
			}
		});
				
		textSnowflakePassword = new JPasswordField();
		textSnowflakePassword.setText(StateControl.getSnowflakePassword());
		textSnowflakePassword.setBounds(101, 200, 939, 26);
		panel_Snowflake.add(textSnowflakePassword);
				
		textSnowflakePassword.getDocument().addDocumentListener(new DocumentListener() {	
			public void changedUpdate(DocumentEvent e) {
				persist();
			}
			public void removeUpdate(DocumentEvent e) {
				persist();
			}
			public void insertUpdate(DocumentEvent e) {
				persist();
			}
			public void persist() {
				StateControl.setSnowflakePassword(String.valueOf(textSnowflakePassword.getPassword()));
				snowflakeConnectionStatus.setText("Not connected");
			}
		});
				
				JLabel lblNewLabel = new JLabel("Account:");
				lblNewLabel.setHorizontalAlignment(SwingConstants.RIGHT);
				lblNewLabel.setBounds(9, 91, 80, 16);
				panel_Snowflake.add(lblNewLabel);
				
				JLabel lblNewLabel_1 = new JLabel("Login ID:");
				lblNewLabel_1.setHorizontalAlignment(SwingConstants.RIGHT);
				lblNewLabel_1.setBounds(9, 167, 80, 16);
				panel_Snowflake.add(lblNewLabel_1);
				
				JLabel lblNewLabel_2 = new JLabel("Password:");
				lblNewLabel_2.setHorizontalAlignment(SwingConstants.RIGHT);
				lblNewLabel_2.setBounds(9, 205, 80, 16);
				panel_Snowflake.add(lblNewLabel_2);
				
				JButton buttonConnectToSnowflake = new JButton("Connect to Snowflake");
				buttonConnectToSnowflake.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						
						connectToSnowflake();
					
					}
				});
				buttonConnectToSnowflake.setBounds(101, 238, 187, 29);
				panel_Snowflake.add(buttonConnectToSnowflake);
				
				JLabel labelSnowflakeConnectionStatus = new JLabel("Not connected");
				labelSnowflakeConnectionStatus.setBounds(289, 243, 751, 16);
				panel_Snowflake.add(labelSnowflakeConnectionStatus);
				snowflakeConnectionStatus = labelSnowflakeConnectionStatus;
				
				JSeparator separator_2 = new JSeparator();
				separator_2.setForeground(Color.GRAY);
				separator_2.setBounds(9, 289, 1031, 12);
				panel_Snowflake.add(separator_2);
				
				JComboBox<String> comboBoxSFDatabase = new JComboBox<String>();
				sfDatabase = comboBoxSFDatabase;
				comboBoxSFDatabase.addItemListener(new ItemListener() {
					public void itemStateChanged(ItemEvent arg0) {
						if (comboBoxSFDatabase.getSelectedIndex() != -1) {
							StateControl.setDatabase(sfDatabase.getSelectedItem().toString());
							textPutStatement.setText("");
							sfSchema.setModel(new DefaultComboBoxModel<String>(SnowflakeObjectInfo.getSchemata(snowflakeConnection, StateControl.getDatabase())));
							Log.log("Selected database: " + StateControl.getDatabase());
						} else {
							sfSchema.setEnabled(false);
						}
						sfSchema.setEnabled(true);
						sfSchema.setSelectedIndex(-1);
						sfStage.setEnabled(false);
					}
				});
				comboBoxSFDatabase.setEnabled(false);
				comboBoxSFDatabase.setBounds(101, 328, 412, 27);
				panel_Snowflake.add(comboBoxSFDatabase);
				
				JLabel lblNewLabel_2_1 = new JLabel("Database:");
				lblNewLabel_2_1.setHorizontalAlignment(SwingConstants.RIGHT);
				lblNewLabel_2_1.setBounds(9, 332, 80, 16);
				panel_Snowflake.add(lblNewLabel_2_1);
				
				JLabel lblNewLabel_2_1_1 = new JLabel("Schema:");
				lblNewLabel_2_1_1.setHorizontalAlignment(SwingConstants.RIGHT);
				lblNewLabel_2_1_1.setBounds(9, 371, 80, 16);
				panel_Snowflake.add(lblNewLabel_2_1_1);
				
				JComboBox<String> comboBoxSFSchema = new JComboBox<String>();
				sfSchema = comboBoxSFSchema;
				comboBoxSFSchema.addItemListener(new ItemListener() {
					public void itemStateChanged(ItemEvent arg0) {
						if (comboBoxSFSchema.getSelectedIndex() != -1) {
							StateControl.setSchema(sfSchema.getSelectedItem().toString());
							textPutStatement.setText("");
							sfStage.setModel(new DefaultComboBoxModel<String>(SnowflakeObjectInfo.getStages(snowflakeConnection, 
									StateControl.getDatabase(), StateControl.getSchema())));
							Log.log("Selected schema: " + StateControl.getSchema());
						} else {
							if (!isInitializing) {
								StateControl.setSchema(null);
								Log.log("Schema set to null");
							}
						}
						sfStage.setEnabled(true);
						sfStage.setSelectedIndex(-1);
					}
				});
				comboBoxSFSchema.setEnabled(false);
				comboBoxSFSchema.setBounds(101, 367, 412, 27);
				panel_Snowflake.add(comboBoxSFSchema);
				
				JLabel lblNewLabel_2_1_2 = new JLabel("Stage:");
				lblNewLabel_2_1_2.setHorizontalAlignment(SwingConstants.RIGHT);
				lblNewLabel_2_1_2.setBounds(9, 410, 80, 16);
				panel_Snowflake.add(lblNewLabel_2_1_2);
				
				JComboBox<String> comboBoxSFStage = new JComboBox<String>();
				sfStage = comboBoxSFStage;
				comboBoxSFStage.addItemListener(new ItemListener() {
					public void itemStateChanged(ItemEvent arg0) {
						if (comboBoxSFStage.getSelectedIndex() != -1) {
							StateControl.setStage(comboBoxSFStage.getSelectedItem().toString());
							textPutStatement.setText("");
							Log.log("Selected stage: " + StateControl.getStage());
						} else {
							if (!isInitializing) {
								StateControl.setStage(null);
								Log.log("Set stage: null");
							}
						}
					}
				});
				comboBoxSFStage.setEnabled(false);
				comboBoxSFStage.setBounds(101, 406, 412, 27);
				panel_Snowflake.add(comboBoxSFStage);
				
				JButton btnRefreshStages = new JButton("Refresh");
				btnRefreshStages.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent arg0) {
						sfStage.setModel(new DefaultComboBoxModel<String>(SnowflakeObjectInfo.getStages(snowflakeConnection, 
								StateControl.getDatabase(), StateControl.getSchema())));
						sfStage.setSelectedIndex(-1);
					}
				});
				btnRefreshStages.setBounds(513, 405, 85, 29);
				panel_Snowflake.add(btnRefreshStages);
				
				JButton btnRefreshDatabases = new JButton("Refresh");
				btnRefreshDatabases.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						try {
							sfDatabase.setModel(new DefaultComboBoxModel<String>(SnowflakeObjectInfo.getDatabases(snowflakeConnection)));
						} catch (IOException e1) {
							Log.logException(e1);
						}
						sfDatabase.setSelectedIndex(-1);
						sfSchema.setEnabled(false);
						sfStage.setEnabled(false);
					}
				});
				btnRefreshDatabases.setBounds(513, 327, 85, 29);
				panel_Snowflake.add(btnRefreshDatabases);
				
				JButton btnRefreshSchemas = new JButton("Refresh");
				btnRefreshSchemas.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						sfSchema.setModel(new DefaultComboBoxModel<String>(SnowflakeObjectInfo.getSchemata(snowflakeConnection, StateControl.getDatabase())));
						sfSchema.setSelectedIndex(-1);
						sfStage.setEnabled(false);
					}
				});
				btnRefreshSchemas.setBounds(513, 366, 85, 29);
				panel_Snowflake.add(btnRefreshSchemas);
				
				JButton btnUnlockPutTab = new JButton("Next >>");
				btnUnlockPutTab.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
					
						String state = ReadyForTab.readyForPutTab();
						if (state.equals(ReadyForTab.READY)) {
							tabs.setEnabledAt(PUT_PANE, true);
							tabbedPane.setSelectedIndex(PUT_PANE);
						} else {
							tabs.setEnabledAt(PUT_PANE, false);
							JOptionPane.showMessageDialog(null, state);
						}
					}
				});
				btnUnlockPutTab.setBounds(905, 479, 135, 29);
				panel_Snowflake.add(btnUnlockPutTab);
				sfDatabase = comboBoxSFDatabase;
				sfSchema = comboBoxSFSchema;
				sfStage = comboBoxSFStage;
				
				JLabel lblNewLabel_6_1_2 = new JLabel("Connection to Snowflake:");
				lblNewLabel_6_1_2.setFont(new Font("Lucida Grande", Font.PLAIN, 16));
				lblNewLabel_6_1_2.setBounds(20, 9, 576, 32);
				panel_Snowflake.add(lblNewLabel_6_1_2);
				
				JButton buttonGetAccountFromURL = new JButton("Get Account from URL");
				buttonGetAccountFromURL.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent arg0) {
						textSnowflakeAccount.setText(UrlParser.SnowflakeAccountFromURL(textSnowflakeAccount.getText()));
					}
				});
				buttonGetAccountFromURL.setBounds(101, 61, 187, 29);
				panel_Snowflake.add(buttonGetAccountFromURL);
				
				JCheckBox checkBoxDoNotStoreCredentials = new JCheckBox("Do not save Snowflake credentials. (Credentials will be in memory only. Will ask for credentials each time the application runs.)");
				checkBoxDoNotStoreCredentials.setSelected(StateControl.getDoNotStoreCredentials());
				checkBoxDoNotStoreCredentials.addItemListener(new ItemListener() {
					public void itemStateChanged(ItemEvent arg0) {
						if (!isInitializing && checkBoxDoNotStoreCredentials.isSelected()) {
							StateControl.setDoNotStoreCredentials("true");
						} else {
							StateControl.setDoNotStoreCredentials("false");					
						}
					}
				});
				checkBoxDoNotStoreCredentials.setBounds(101, 138, 926, 23);
				panel_Snowflake.add(checkBoxDoNotStoreCredentials);
				
				JPanel panel_PUT = new JPanel();
				tabbedPane.addTab("Set Put Options", null, panel_PUT, null);
				tabbedPane.setDisabledIconAt(2, new ImageIcon(MainForm.class.getResource(RESOURCE_PATH + "/images/lock.png")));
				tabbedPane.setEnabledAt(2, true);
				
				tabs.setEnabledAt(2, false);
				panel_PUT.setLayout(null);
				
				JComboBox<String> comboBoxCompression = new JComboBox<String>();
				sourceCompressionComboBox = comboBoxCompression;
				comboBoxCompression.setSelectedItem(StateControl.getSourceCompression());
				comboBoxCompression.addItemListener(new ItemListener() {
					public void itemStateChanged(ItemEvent arg0) {
						if (comboBoxCompression.getSelectedIndex() != -1) {
							textPutStatement.setText("");
							StateControl.setSourceCompression(comboBoxCompression.getSelectedItem().toString());
						}
					}
				});
				comboBoxCompression.setModel(new DefaultComboBoxModel<String>(new String[] {"NONE", "AUTO_DETECT", "GZIP", "BZ2", "BROTLI", "ZSTD", "DEFLATE", "RAW_DEFLATE"}));
				comboBoxCompression.setBounds(221, 93, 195, 27);
				panel_PUT.add(comboBoxCompression);
				comboBoxCompression.setSelectedIndex(-1);
				
				JLabel lblNewLabel_10 = new JLabel("Source File Compression");
				lblNewLabel_10.setBounds(45, 97, 174, 16);
				panel_PUT.add(lblNewLabel_10);
				
				JCheckBox chckbxOverwriteExistingFiles = new JCheckBox("Overwrite existing files in the stage.");
				chckbxOverwriteExistingFiles.setSelected(StateControl.getOverwriteExisting());
				chckbxOverwriteExistingFiles.addItemListener(new ItemListener() {
					public void itemStateChanged(ItemEvent e) {
						StateControl.setOverwriteExisting(
								chckbxOverwriteExistingFiles.isSelected() == true ? "TRUE" : "FALSE");
						textPutStatement.setText("");
					}
				});
				chckbxOverwriteExistingFiles.setBounds(35, 62, 363, 23);
				panel_PUT.add(chckbxOverwriteExistingFiles);
				
				JScrollPane scrollPane_1 = new JScrollPane();
				scrollPane_1.setBounds(16, 183, 1008, 269);
				panel_PUT.add(scrollPane_1);
				
				JTextArea textAreaPutStatement = new JTextArea();
				textPutStatement = textAreaPutStatement;
				textAreaPutStatement.setFont(new Font("Courier New", Font.BOLD, 13));
				textAreaPutStatement.setEnabled(false);
				scrollPane_1.setViewportView(textAreaPutStatement);
				
				textAreaPutStatement.getDocument().addDocumentListener(new DocumentListener() {	
					  public void changedUpdate(DocumentEvent e) {
					    persist();
					  }
					  public void removeUpdate(DocumentEvent e) {
					    persist();
					  }
					  public void insertUpdate(DocumentEvent e) {
					    persist();
					  }
					  public void persist() {
						 StateControl.setPutStatement(textAreaPutStatement.getText());
					  }
					});
				
				JCheckBox chckbxPutTextLocled = new JCheckBox("Locked from manual editing");
				chckbxPutTextLocled.addItemListener(new ItemListener() {
					public void itemStateChanged(ItemEvent arg0) {			
					textAreaPutStatement.setEnabled(!chckbxPutTextLocled.isSelected());
					}
				});
				chckbxPutTextLocled.setSelected(true);
				chckbxPutTextLocled.setBounds(814, 148, 209, 23);
				panel_PUT.add(chckbxPutTextLocled);
				
				JLabel lblNewLabel_7_1 = new JLabel("PUT Statement:");
				lblNewLabel_7_1.setBounds(16, 147, 153, 16);
				panel_PUT.add(lblNewLabel_7_1);
				
				JButton btnGeneratePutStatement = new JButton("Generate PUT Statement");
				btnGeneratePutStatement.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent arg0) {
					
						String ready = StateControl.readyForPutGeneration();
						
						if (ready.compareTo(StateControl.READY) == 0) {
							PutStatementGenerator gen = new PutStatementGenerator();
							gen.setDatabase(StateControl.getDatabase());
							gen.setSchema(StateControl.getSchema());
							gen.setStage(StateControl.getStage());
							gen.setSourceCompression(StateControl.getSourceCompression());
							gen.setOverwrite(chckbxOverwriteExistingFiles.isSelected());
							gen.setOverwrite(StateControl.getOverwriteExisting());
							textAreaPutStatement.setText(gen.generate());								
						} else {
							MessageBox.show(frame, ready, "Not Ready to Generate PUT Statement");
						}
					}
				});
				btnGeneratePutStatement.setBounds(120, 142, 197, 29);
				panel_PUT.add(btnGeneratePutStatement);
				
				JButton btnUnlockCopyTab = new JButton("Next >>");
				btnUnlockCopyTab.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent arg0) {
						StateControl.setPutStatement(textAreaPutStatement.getText());
						String state = StateControl.readyToLoad();
						if (state.equals("Ready")) {
							tabs.setEnabledAt(LOAD_PANE, true);
							tabbedPane.setSelectedIndex(LOAD_PANE);
						} else {
							tabs.setEnabledAt(LOAD_PANE, false);
							JOptionPane.showMessageDialog(null, state);
						}
					}
				});
				btnUnlockCopyTab.setBounds(905, 479, 135, 29);
				panel_PUT.add(btnUnlockCopyTab);
				
				JLabel lblNewLabel_11 = new JLabel("NOTE: Do not use AUTO_DETECT for uncompressed source files. Use \"NONE\".");
				lblNewLabel_11.setBounds(430, 97, 526, 16);
				panel_PUT.add(lblNewLabel_11);
		
	    fileInventoryTimer = new Timer(100, new ActionListener() {		
	        public void actionPerformed(ActionEvent evt) {
	        	if(fileInventory.isAlive()) {
	        		String fileCount = Long.toString(StateControl.getInventoriedFileCount());
	        		fileInventoryLabel.setText("Inventoried " + fileCount + " files...");
	        	} else {
	        		filesInventory.setText("Inventory has " + StateControl.getInventoriedFileCount() + " files.");
	        		inventoryFiles.setEnabled(true);
	        		fileInventoryTimer.stop();
	        	}
	        }
	    });
	    
		textAreaPutStatement.getDocument().addDocumentListener(new DocumentListener() {	
			  public void changedUpdate(DocumentEvent e) {
			    persist();
			  }
			  public void removeUpdate(DocumentEvent e) {
			    persist();
			  }
			  public void insertUpdate(DocumentEvent e) {
			    persist();
			  }
			  public void persist() {
				 StateControl.setPutStatement(textAreaPutStatement.getText());
			  }
			});
		tabbedPane.setDisabledIconAt(0, new ImageIcon(MainForm.class.getResource(RESOURCE_PATH + "/images/lock.png")));
		panel_Files.setLayout(null);

		inventoryFiles = buttonInventoryFiles;
		filesInventory = lblFileInventory;
		filesInventory.setText("Inventory has " + StateControl.getInventoriedFileCount() + " files.");
		
		JLabel lblNewLabel_6_1_1 = new JLabel("Inventory Local Files:");
		lblNewLabel_6_1_1.setFont(new Font("Lucida Grande", Font.PLAIN, 16));
		lblNewLabel_6_1_1.setBounds(20, 359, 576, 32);
		panel_Files.add(lblNewLabel_6_1_1);
		
		JLabel lblNewLabel_6_1_1_1 = new JLabel("Pattern Match Files (Optional):");
		lblNewLabel_6_1_1_1.setFont(new Font("Lucida Grande", Font.PLAIN, 16));
		lblNewLabel_6_1_1_1.setBounds(20, 180, 576, 32);
		panel_Files.add(lblNewLabel_6_1_1_1);
				
		JButton btnEditSQLiteDB = new JButton("Unlock Database");
		btnEditSQLiteDB.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
			
				try {
					SqliteManager.closeConnection();
					MessageBox.show(frame, "Database unlocked. Path is: \n" + SqliteManager.getDbPath(), "Unlock Database");
				} catch (SQLException e) {
					Log.logException(e);
				}	
			}
		});
		btnEditSQLiteDB.setBounds(181, 464, 163, 29);
		panel_Files.add(btnEditSQLiteDB);
		
		JPanel panel_Loader = new JPanel();
		tabbedPane.addTab("Load Files", null, panel_Loader, null);
		tabbedPane.setEnabledAt(3, false);
		tabbedPane.setDisabledIconAt(3, new ImageIcon(MainForm.class.getResource(RESOURCE_PATH + "/images/lock.png")));
		panel_Loader.setLayout(null);
		
		comboBoxLoaderThreads = new JComboBox<Integer>();
		comboBoxLoaderThreads.addItemListener(new ItemListener() {
			public void itemStateChanged(ItemEvent arg0) {
				if(!isInitializing) {
					StateControl.setThreadCount(comboBoxLoaderThreads.getSelectedItem().toString());	
				}
			}
		});
		comboBoxLoaderThreads.setModel(new DefaultComboBoxModel<Integer>(new Integer[] 
				{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}));
		comboBoxLoaderThreads.setSelectedIndex(2);
		comboBoxLoaderThreads.setBounds(184, 25, 135, 27);
		panel_Loader.add(comboBoxLoaderThreads);
		
		JLabel lblNewLabel_12 = new JLabel("Parallel PUT Uploads:");
		lblNewLabel_12.setHorizontalAlignment(SwingConstants.RIGHT);
		lblNewLabel_12.setBounds(6, 29, 166, 16);
		panel_Loader.add(lblNewLabel_12);
		
		JButton btnStopLoad = new JButton("Stop Load");
		btnStopLoad.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
			
				FileLoader.stop();
				btnStopLoad.setEnabled(false);
				
			}
		});
		btnStopLoad.setEnabled(false);
		btnStopLoad.setBounds(758, 479, 135, 29);
		panel_Loader.add(btnStopLoad);
		
		JProgressBar progressBarFileStaging = new JProgressBar();
		progressBarFileStaging.setBounds(6, 176, 1034, 20);
		panel_Loader.add(progressBarFileStaging);
		
		JProgressBar progressBarGBStaging = new JProgressBar();
		progressBarGBStaging.setBounds(6, 222, 1034, 20);
		panel_Loader.add(progressBarGBStaging);
		
			JButton btnStartLoad = new JButton("Start Load");
			btnStartLoad.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent arg0) {
					StateControl.setPutStatement(textAreaPutStatement.getText());
					btnStartLoad.setEnabled(false);
					btnStopLoad.setEnabled(false);
					try {
						Connection conn = SqliteManager.getConnection();
						Statement stmt = conn.createStatement();
						ResultSet rs = stmt.executeQuery("select * from GET_STAGE_INFO;");
						progressBarFileStaging.setMaximum(rs.getInt("FILES_TO_STAGE"));
						bytesToStage = rs.getLong("BYTES_TO_STAGE");
						progressBarGBStaging.setMaximum(1000);
					} catch (SQLException e) {
						Log.logException(e);
					} catch (ClassNotFoundException e) {
						Log.logException(e);
					}
					FileLoader.loadFiles(StateControl.getThreadCount());
					fileStagerTimer.start();
				}
			});
			btnStartLoad.setBounds(905, 479, 135, 29);
			panel_Loader.add(btnStartLoad);
			
			JLabel lblFileStaging = new JLabel("Files Staged:");
			lblFileStaging.setBounds(6, 161, 1034, 16);
			panel_Loader.add(lblFileStaging);
			
			JLabel lblFileLoading = new JLabel("Data Staged:");
			lblFileLoading.setBounds(6, 208, 1034, 16);
			panel_Loader.add(lblFileLoading);
			
			JLabel lblNewLabel_6 = new JLabel("Files staged:");
			lblNewLabel_6.setHorizontalAlignment(SwingConstants.RIGHT);
			lblNewLabel_6.setBounds(57, 92, 113, 16);
			panel_Loader.add(lblNewLabel_6);
			
			JLabel lblNewLabel_6_2 = new JLabel("GB staged:");
			lblNewLabel_6_2.setHorizontalAlignment(SwingConstants.RIGHT);
			lblNewLabel_6_2.setBounds(59, 120, 113, 16);
			panel_Loader.add(lblNewLabel_6_2);
			
			JLabel lblFilesStaged = new JLabel("0");
			lblFilesStaged.setBounds(194, 92, 373, 16);
			panel_Loader.add(lblFilesStaged);
			
			JLabel lblGbStaged = new JLabel("0");
			lblGbStaged.setBounds(194, 120, 373, 16);
			panel_Loader.add(lblGbStaged);
			
			JLabel lblNewLabel_6_3 = new JLabel("Staging threads:");
			lblNewLabel_6_3.setHorizontalAlignment(SwingConstants.RIGHT);
			lblNewLabel_6_3.setBounds(57, 64, 115, 16);
			panel_Loader.add(lblNewLabel_6_3);
			
			JLabel lblStagingThreads = new JLabel("0");
			lblStagingThreads.setBounds(194, 64, 373, 16);
			panel_Loader.add(lblStagingThreads);
			
		    fileStagerTimer = new Timer(100, new ActionListener() {
		        public void actionPerformed(ActionEvent evt) {

		        	lblGbStaged.setText(Double.toString(FileLoader.getGigabytesStaged()));
		        	lblFilesStaged.setText(Long.toString(FileLoader.getFilesStaged()));
		       		lblStagingThreads.setText(Integer.toString(FileLoader.threadsStaging()));
		       		progressBarFileStaging.setValue((int)FileLoader.getFilesStaged());
		       		progressBarGBStaging.setValue((int)(((double)FileLoader.getBytesStaged() / (double)bytesToStage) * 1000.0));
		       		btnStartLoad.setEnabled(!FileLoader.isRunning());
		       		btnStopLoad.setEnabled(FileLoader.isRunning() && !FileLoader.isStopped());
		        	if (!FileLoader.isRunning()) {
		        		fileStagerTimer.stop();
		        		MessageBox.show(frame, "Staged all files.", "Staging Complete");
		        	}
		        }
		    });
			
	}
	
	
	
/*******************************************************************************************************************/
	
	private void finalInit() {
		setPutText();
		setThreadCount();
		if (StateControl.getSourceCompression() != null) {
			setSourceCompression();
		}
		try {
			if (snowflakeConnection.isConnected()) {
				connectToSnowflake();
				if(StateControl.getDatabase() != null) {
					setDatabase();
					setSchema();
					setStage();
				}
			}
		} catch (SQLException e) {
			Log.logException(e);
		}
	}
	
	private void setThreadCount() {
		for (int i = 0; i < comboBoxLoaderThreads.getItemCount(); i++) {
			if (comboBoxLoaderThreads.getItemAt(i).toString().toUpperCase().compareTo(
					Integer.toString(StateControl.getThreadCount()).toUpperCase()) == 0) {
				comboBoxLoaderThreads.setSelectedIndex(i);
			}
		}
	}
	
	private void setPutText() {
		if (StateControl.getPutStatement() != null) {
			textPutStatement.setText(StateControl.getPutStatement());	
		}
	}

	private void setSourceCompression() {
		for (int i = 0; i < sourceCompressionComboBox.getItemCount(); i++) {
			if (sourceCompressionComboBox.getItemAt(i).toString().toUpperCase().compareTo(
					StateControl.getSourceCompression().toUpperCase()) == 0) {
				sourceCompressionComboBox.setSelectedIndex(i);
			}
		}
	}
	
	private void setDatabase() {
		for (int i = 0; i < sfDatabase.getItemCount(); i++) {
			if (sfDatabase.getItemAt(i).toString().compareTo(StateControl.getDatabase()) == 0) {
				sfDatabase.setSelectedIndex(i);
				sfSchema.setEnabled(true);
			}
		}
	}
	
	private void setSchema() {
		if (sfDatabase.getSelectedIndex() != -1) {
			for (int i = 0; i < sfSchema.getItemCount(); i++) {
				if (sfSchema.getItemAt(i).toString().compareTo(StateControl.getSchema()) == 0) {
					sfSchema.setSelectedIndex(i);
					sfStage.setEnabled(true);
				}
			}
		}
	}
	
	private void setStage() {
		if (sfSchema.getSelectedIndex() != -1) {
			for (int i = 0; i < sfStage.getItemCount(); i++) {
				if (sfStage.getItemAt(i).toString().compareTo(StateControl.getStage()) == 0) {
					sfStage.setSelectedIndex(i);
				}
			}
		}
	}
	
	private void connectToSnowflake() {
		snowflakeConnection.setAccountURL(StateControl.getSnowflakeAccount());
		snowflakeConnection.setPassword(StateControl.getSnowflakePassword());
		snowflakeConnection.setUser(StateControl.getSnowflakeLogin());
		
		try {
			//This works with built-in Snowflake DUO MFA. Additional support for MFA.
			//---------> //Properties props = new Properties(); <------------- Multi-Factor Authentication.
			//props.put("passcode", "12345678");
			//snowflakeConnection.setConnectionProperties(props);
			
			snowflakeConnection.getConnection();
			snowflakeConnectionStatus.setText("Connected to Snowflake.");
			StateControl.connectionStatus(true);
			sfDatabase.setModel(new DefaultComboBoxModel<String>(SnowflakeObjectInfo.getDatabases(snowflakeConnection)));
			sfDatabase.setEnabled(true);
			sfDatabase.setSelectedIndex(-1);
			sfSchema.setEnabled(false);
		}
		catch(SQLException err) {
			snowflakeConnectionStatus.setText(err.getMessage());
			StateControl.connectionStatus(false);
			err.printStackTrace();
		}
		catch(IllegalArgumentException err) {
			snowflakeConnectionStatus.setText(err.getMessage());
			StateControl.connectionStatus(false);
			err.printStackTrace();
		} catch (IOException err) {
			snowflakeConnectionStatus.setText(err.getMessage());
			StateControl.connectionStatus(false);
			err.printStackTrace();
		} catch (ClassNotFoundException err) {
			snowflakeConnectionStatus.setText(err.getMessage());
			StateControl.connectionStatus(false);
			err.printStackTrace();
		}
	}
}
