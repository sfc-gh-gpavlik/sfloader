/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.sfloader;

import com.snowflake.community.utils.Log;

public class FileLoader {

	private static Thread[] fileStager;
	private static int fileStageThreads;
	private static long filesStaged;
	private static long bytesStaged;
	private static boolean _isStopped;
	
	public static void loadFiles(int numberOfThreads) {
		_isStopped = false;
		fileStageThreads = numberOfThreads;
		fileStager = new FileStager[numberOfThreads];
		for (int i = 0; i < numberOfThreads; i++) {
			fileStager[i] = new FileStager(i);
			fileStager[i].start();
		}
	}
	
	public static boolean isRunning() {
		return threadsStaging() > 0;
	}

	public static int threadsStaging() {
		int threadsRunning = 0;
		for(int i = 0; i < fileStageThreads; i++) {
			if (fileStager[i].isAlive()) threadsRunning++;
		}
		return threadsRunning;
	}

	public static long getFilesStaged() {
		return filesStaged;
	}
	
	public static void incrementFilesStaged() {
		filesStaged++;
		Log.log(filesStaged + (filesStaged > 1 ? " files" : " file") + " put to stage.");
	}
	
	public static long getBytesStaged() {
		return bytesStaged;
	}
	
	public static double getGigabytesStaged() {
		return (double)bytesStaged / 1000000000.0;
	}
	
	public static void addBytesStaged(long bytesAdded) {
		bytesStaged += bytesAdded;
	}
	
	public static void stop() {
		_isStopped = true;
	}
	
	public static boolean isStopped() {
		return _isStopped;
	}
}
