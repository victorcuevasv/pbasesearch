package org.dataone.daks.pbaserdf.dao;

import java.io.File;
import java.util.Arrays;

import com.sleepycat.bind.serial.*;
import com.sleepycat.collections.*;
import com.sleepycat.je.*;


public class SearchIndex {
	
	
	private static final String CLASS_CATALOG = "search_catalog";
	private String directory;
	private Environment env;
	private StoredClassCatalog catalog;
	private Database cacheDb;
	private Database catalogDb;
	private StoredMap storedMap;
	
	private static final SearchIndex instance = new SearchIndex();
	
	
	public SearchIndex() {

	}
	
	
	public static SearchIndex getInstance() {
    	return instance;
    }
	
	
	public synchronized void init(String directory) {
		if( this.storedMap == null || ( this.directory != null && ! this.directory.equals(directory) ) ) {
			if( this.storedMap != null )
				this.shutdown();
			this.directory = directory;
			try {
				EnvironmentConfig envConfig = new EnvironmentConfig();
				envConfig.setTransactional(false);
				envConfig.setAllowCreate(true);
				this.env = new Environment(new File(this.directory), envConfig);
				DatabaseConfig dbConfig = new DatabaseConfig();
				dbConfig.setTransactional(false);
				dbConfig.setAllowCreate(true);
				this.catalogDb = this.env.openDatabase(null, CLASS_CATALOG, dbConfig);
				this.catalog = new StoredClassCatalog(this.catalogDb);
				SerialBinding fKey = new SerialBinding(this.catalog, String.class);
				SerialBinding fValue = new SerialBinding(this.catalog, String.class);
				this.storedMap = new StoredMap(this.catalogDb, fKey, fValue, true);
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	
	public synchronized void shutdown() {
		try {
			this.catalog.close();
			this.env.close();
		}
		catch (DatabaseException e) {
			e.printStackTrace();
		}
		this.storedMap = null;
	}
	
	
	public void put(String key, String value) {
		this.storedMap.put(key, value);
	}
	
	
	public void replace(String key, String value) {
		this.storedMap.replace(key, value);
	}
	
	
	public String get(String key) {
		String retVal = null;
		//if( this.storedMap.containsKey(key) )
		retVal = (String)this.storedMap.get(key);
		return retVal;
	}
	
	
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		Cursor cursor = null;
		try { 
		    cursor = this.catalogDb.openCursor(null, null);
		    DatabaseEntry foundKey = new DatabaseEntry();
		    DatabaseEntry foundData = new DatabaseEntry();
		    while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
		    	byte[] keyArray = foundKey.getData();
		        String keyString = "NULL";
		        if( keyArray.length > 3 )
		        	keyString = new String( Arrays.copyOfRange(keyArray, 3, keyArray.length ) );
		        byte[] dataArray = foundData.getData();
		        String dataString = "NULL";
		        if( dataArray.length > 3 )
		        	dataString = new String( Arrays.copyOfRange(dataArray, 3, dataArray.length ) );
		        buffer.append(keyString + " | " + dataString + "\n");
		    }
		    cursor.close();
		}
		catch (DatabaseException de) {
		    System.err.println("Error accessing database." + de);
		}
		return buffer.toString();
	}
	
	
}




