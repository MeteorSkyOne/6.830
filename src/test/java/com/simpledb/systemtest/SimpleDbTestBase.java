package com.simpledb.systemtest;

import org.junit.Before;

import com.simpledb.common.Database;

/**
 * Base class for all SimpleDb test classes. 
 * @author nizam
 *
 */
public class SimpleDbTestBase {
	/**
	 * Reset the database before each test is run.
	 */
	@Before	public void setUp() throws Exception {					
		Database.reset();
	}
	
}
