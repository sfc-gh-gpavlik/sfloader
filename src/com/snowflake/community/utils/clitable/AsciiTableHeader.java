/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.utils.clitable;

public class AsciiTableHeader {

	private String 	_headerText;
	private int 	_headerAlignment;
	private int 	_dataAlignment;
	
	/**
	 * Creates a column header for an ASCII-based table, which includes header and alignment information
	 * @param headerText The text to display for the column's header
	 * @param dataAlignment The alignment for the column's data, right, left, center, or auto
	 * @param headerAlignment The alignment for the column's header, right, left, or center
	 */
	public AsciiTableHeader(String headerText, int dataAlignment, int headerAlignment) {
		setHeaderText(headerText);
		setDataAlignment(dataAlignment);
		setHeaderAlignment(headerAlignment);
	}
	
	/**
	 * Creates a column header for an ASCII-based table, which includes header and alignment information
	 * @param headerText The text to display for the column's header
	 * @param dataAlignment The alignment for the column's data, right, left, center, or auto
	 */
	public AsciiTableHeader(String headerText, int dataAlignment) {
		setHeaderText(headerText);
		setDataAlignment(dataAlignment);
		setHeaderAlignment(AsciiTable.ALIGN_CENTER);
	}
	
	/**
	 * Creates a column header for an ASCII-based table, which includes header and alignment information
	 * @param headerText The text to display for the column's header
	 */
	public AsciiTableHeader(String headerText) {
		setHeaderText(headerText);
		setDataAlignment(AsciiTable.ALIGN_AUTO);
		setHeaderAlignment(AsciiTable.ALIGN_CENTER);
	}

	public String getHeaderText() {
		return _headerText;
	}

	public void setHeaderText(String headerText) {
		_headerText = headerText;
	}

	public int getHeaderAlignment() {
		return _headerAlignment;
	}

	public void setHeaderAlignment(int headerAlignment) {
		_headerAlignment = headerAlignment;
	}

	public int getDataAlignment() {
		return _dataAlignment;
	}

	public void setDataAlignment(int dataAlignment) {
		_dataAlignment = dataAlignment;
	}
}
