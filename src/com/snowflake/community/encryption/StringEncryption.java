/*
 * Copyright (c) 2020 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.community.encryption;

import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

public class StringEncryption {
	
	private static final String key = "LKwpRq2Kxu/mPi4N7D0M9DzLO0tUH8/sjjsoH8WAJws=";
	
	public static SecretKey getSecretKey() throws NoSuchAlgorithmException {
		KeyGenerator keyGen = KeyGenerator.getInstance("AES");
		keyGen.init(256);
		return keyGen.generateKey();
	}	
	
	/**
	 * Encrypts a string using AES-256 encryption and returns it as a base-64 encoded string
	 * @param input The string to encrypt
	 * @return A base-64 encoded string holding the encrypted string
	 */
	public static String encryptStringToBase64(String input) {
		if (input == null || input.equals("")) return "";
		try {
			Key aesKey = new SecretKeySpec(Base64.getDecoder().decode(key.getBytes()), "AES");
			Cipher cipher = Cipher.getInstance("AES");
			cipher.init(Cipher.ENCRYPT_MODE, aesKey);
			byte[] encrypted = cipher.doFinal(input.getBytes());
			return new String(Base64.getEncoder().encode(encrypted));
		}
		catch (Exception e) {
			e.printStackTrace();
			return null;
		}	
	}
	
	/**
	 * Decrypts a base-64 encoded, encrypted string using AES encryption
	 * @param encryptedString The base-64 encoded representation of the encrypted string
	 * @return A decrypted, decoded string
	 */
	public static String decryptString(String encryptedString) {
		if (encryptedString == null || encryptedString.equals("")) return "";
		try {
			byte[] encryptedBytes = Base64.getDecoder().decode(encryptedString.getBytes());
			Key aesKey = new SecretKeySpec(Base64.getDecoder().decode(key.getBytes()), "AES");
			Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.DECRYPT_MODE, aesKey);
            return new String(cipher.doFinal(encryptedBytes));
		}
		catch (Exception e) {
			e.printStackTrace();
			return null;
		}		
	}
}
