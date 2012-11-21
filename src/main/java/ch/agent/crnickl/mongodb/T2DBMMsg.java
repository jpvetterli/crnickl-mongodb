/*
 *   Copyright 2012 Hauser Olsson GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Package: ch.agent.crnickl.jdbc
 * Type: T2DBJMsg
 * Version: 1.0.0
 */
package ch.agent.crnickl.mongodb;

import java.util.ResourceBundle;

import ch.agent.core.KeyedMessage;
import ch.agent.core.MessageBundle;

/**
 * T2DBJMsg provides keyed messages to the package. 
 * 
 * @author Jean-Paul Vetterli
 * @version 1.0.0
 */
public class T2DBMMsg extends KeyedMessage {
	
	/**
	 * Message symbols.
	 */
	public class J {
		public static final String J80020 = "J80020"; 
		public static final String J80050 = "J80050"; 
		public static final String J80100 = "J80100"; 
		public static final String J81010 = "J81010"; 
		public static final String J81012 = "J81012"; 
		public static final String J81014 = "J81014"; 
		public static final String J81016 = "J81016"; 
		public static final String J81020 = "J81020"; 
		public static final String J81021 = "J81021"; 
	}
	
	private static final String BUNDLE_NAME = ch.agent.crnickl.mongodb.T2DBMMsg.class.getName();
	
	private static final MessageBundle BUNDLE = new MessageBundle("T2DBJ",
			ResourceBundle.getBundle(BUNDLE_NAME));

	/**
	 * Return a keyed exception.
	 * 
	 * @param key a key
	 * @param arg zero or more arguments
	 * @return a keyed exception
	 */
	public static T2DBMException exception(String key, Object... arg) {
		return new T2DBMException(new T2DBMMsg(key, arg));
	}

	/**
	 * Return a keyed exception.
	 * 
	 * @param cause the exception's cause
	 * @param key a key
	 * @param arg zero or more arguments
	 * @return a keyed exception
	 */
	public static T2DBMException exception(Throwable cause, String key, Object... arg) {
		return new T2DBMException(new T2DBMMsg(key, arg), cause);
	}

	/**
	 * Construct a keyed message.
	 * 
	 * @param key a key
	 * @param args zero or more arguments
	 */
	public T2DBMMsg(String key, Object... args) {
		super(key, BUNDLE, args);
	}

}
