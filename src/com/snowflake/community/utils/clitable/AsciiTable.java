/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.utils.clitable;

import com.snowflake.community.utils.StringUtils;

public class AsciiTable {

	public static final int ALIGN_LEFT   = 0;
	public static final int ALIGN_RIGHT  = 1;
	public static final int ALIGN_CENTER = 2;
	public static final int ALIGN_AUTO   = 3;		// Aligns strings left, numbers right.
	
	private static final int LEFT = 0;
	private static final int RIGHT = 1;
	
	/**
	 * Prints an ASCII-based table in the command line interpreter (terminal)
	 * @param header The header array for the table, which includes column headers and alignment information
	 * @param data The data for the table
	 */
	public static void printTable(AsciiTableHeader[] header, String[][] data) {
		int rows = data.length;
		int cols = header.length;
		int longestCol[] = getLongestColumns(header, data, cols, rows);
		writeHeader(longestCol, header, cols);
		writeTable(header, data, longestCol);
	}
	
	private static void writeHeader(int[] longestCol, AsciiTableHeader[] header, int cols ) {
		String[] padding;
		writeLine(longestCol);		
		System.out.print("|");
		for(int col = 0; col < cols; col++) {
			padding = getPadding(header[col].getHeaderAlignment(), longestCol[col], header[col].getHeaderText());
			System.out.print(" " + padding[LEFT] + header[col].getHeaderText() + padding[RIGHT] + " |");
		}
		System.out.println();
		writeLine(longestCol);
	}
	
	private static void writeTable(AsciiTableHeader[] header, String[][] data, int[] longestCol) {
		String[] padding;
		for(int row = 0; row < data.length; row++) {
			System.out.print("|");
			for(int col = 0; col < data[row].length; col++) {
				padding = getPadding(header[col].getDataAlignment(), longestCol[col], data[row][col]);	
				System.out.print(" " + padding[LEFT] + data[row][col] + padding[RIGHT] + " |");
			}
			System.out.println();
		}
		writeLine(longestCol);
	}
	
	private static String[] getPadding(int alignment, int columnWidth, String value) {
		String[] padding = new String[2];
		int leftPadding;
		int rightPadding;
		switch (alignment) {
			case AsciiTable.ALIGN_LEFT:
				padding[LEFT] = "";
				padding[RIGHT] = new String(new char[columnWidth - value.length()]).replace("\0", " ");
				break;
			case AsciiTable.ALIGN_RIGHT:
				padding[LEFT] = new String(new char[columnWidth - value.length()]).replace("\0", " ");
				padding[RIGHT] = "";
				break;
			case AsciiTable.ALIGN_CENTER:
				leftPadding = (columnWidth - value.length()) / 2;
				rightPadding = (columnWidth - value.length()) - leftPadding;
				padding[LEFT] = new String(new char[leftPadding]).replace("\0", " ");
				padding[RIGHT] = new String(new char[rightPadding]).replace("\0", " ");;
				break;
			case AsciiTable.ALIGN_AUTO:  // FOR NOW JUST ALIGN LEFT!!! recurse this method based on isNumeric
				if(StringUtils.isNumeric(value)){
					padding = getPadding(AsciiTable.ALIGN_RIGHT, columnWidth, value);
				} else {
					padding = getPadding(AsciiTable.ALIGN_LEFT, columnWidth, value);
				}
				break;
		}
		return padding;
	}
	
	private static int[] getLongestColumns(AsciiTableHeader[] header, String[][] data, int cols, int rows) {
		int longestCol[] = new int[cols];
		for(int row = 0; row < rows; row++) {
			for(int col = 0; col < data[row].length; col++) {
				if (data[row][col].length() > longestCol[col]) {
					longestCol[col] = data[row][col].length();
				}
			}
		}
		for(int col = 0; col < cols; col++) {
			if (header[col].getHeaderText().length() > longestCol[col]) {
				longestCol[col] = header[col].getHeaderText().length();
			}
		}
		return longestCol;
	}
	
	private static void writeLine(int[] longestCol) {
		String fill;
		for(int col = 0; col < longestCol.length; col++) {
			System.out.print("+");
			fill = new String(new char[longestCol[col] + 2]).replace("\0", "-");
			System.out.print(fill);
		}
		System.out.println("+");
	}
}
