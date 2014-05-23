package org.dataone.daks.pbaserdf.dao;

import java.io.File;

import com.sleepycat.bind.serial.*;
import com.sleepycat.collections.*;
import com.sleepycat.je.*;


public class SearchIndex {
	
	
	private static final String CLASS_CATALOG = "search_catalog";
	private String directory;
	private Environment env;
	private StoredClassCatalog catalog;
	private Database cacheDb;
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
				Database catalogDb = env.openDatabase(null, CLASS_CATALOG, dbConfig);
				this.catalog = new StoredClassCatalog(catalogDb);
				SerialBinding fKey = new SerialBinding(catalog, String.class);
				SerialBinding fValue = new SerialBinding(catalog, String.class);
				this.storedMap = new StoredMap(catalogDb, fKey, fValue, true);
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
	
	
	public String get(String key) {
		String retVal = null;
		//if( this.storedMap.containsKey(key) )
		retVal = (String)this.storedMap.get(key);
		return retVal;
	}
	
	
}




