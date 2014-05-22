package org.dataone.daks.pbasesearch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Hashtable;
import java.util.Random;


public class APISCatalog {

	
	List<String> genericAPINames;
	List<String> specificAPINames;
	Hashtable<String, ServiceAPI> genericAPISHT;
	Hashtable<String, ServiceAPI> specificAPISHT;
	private static final double GENERIC_PROB = 0.3;
	private static final double SPECIFIC_PROB = 0.7;
	private Random rand;
	
	
	public APISCatalog(String apisFolder, String genericApisFile, String specificApisFile) {
		this.rand = new Random();
		this.genericAPINames = this.readFileAsList(genericApisFile);
		this.specificAPINames = this.readFileAsList(specificApisFile);
		this.genericAPISHT = new Hashtable<String, ServiceAPI>();
		this.specificAPISHT = new Hashtable<String, ServiceAPI>();
		for( String apiName: this.genericAPINames ) {
			ServiceAPI serviceAPI = new ServiceAPI(apisFolder + "/" + apiName + ".xml");
			this.genericAPISHT.put(apiName, serviceAPI);
		}
		for( String apiName: this.specificAPINames ) {
			ServiceAPI serviceAPI = new ServiceAPI(apisFolder + "/" + apiName + ".xml");
			this.specificAPISHT.put(apiName, serviceAPI);
		}
	}
	
	
	private String getRandomGenericService() {
		int randPosition = this.randInt(0, this.genericAPINames.size()-1);
		String genericApiName = this.genericAPINames.get(randPosition);
		ServiceAPI genericApi = this.genericAPISHT.get(genericApiName);
		randPosition = this.randInt(0, genericApi.getNServices()-1);
		String service = genericApi.getServiceNameAt(randPosition);
		return genericApiName + ":" + service;
	}
	
	
	private String getRandomSpecificService(String specificApiName) {
		ServiceAPI specificApi = this.specificAPISHT.get(specificApiName);
		int randPosition = this.randInt(0, specificApi.getNServices()-1);
		String service = specificApi.getServiceNameAt(randPosition);
		return specificApiName + ":" + service;
	}
	
	
	public String getRandomSpecificAPIName() {
		int randPosition = this.randInt(0, this.specificAPINames.size()-1);
		String specificApiName = this.specificAPINames.get(randPosition);
		return specificApiName;
	}
	
	
	public String getRandomService(String defaultSpecificAPIName) {
		double dice = this.randDouble(0, 1.0);
		String retVal = null;
		if( dice < this.GENERIC_PROB )
			retVal = this.getRandomGenericService();
		else
			retVal = this.getRandomSpecificService(defaultSpecificAPIName);
		return retVal;
	}
	
	
	public ServiceAPI getGenericAPI(String name) {
		return this.genericAPISHT.get(name);
	}
	
	
	public ServiceAPI getSpecificAPI(String name) {
		return this.specificAPISHT.get(name);
	}
	
	
	public int getNGenericAPIS() {
		return this.genericAPINames.size();
	}
	
	
	public int getNSpecificAPIS() {
		return this.specificAPINames.size();
	}
	
	
	public ServiceAPI getGenericAPIAt(int position) {
		String name = this.genericAPINames.get(position);
		return this.genericAPISHT.get(name);
	}
	
	
	public ServiceAPI getSpecificAPIAt(int position) {
		String name = this.specificAPINames.get(position);
		return this.specificAPISHT.get(name);
	}
	
	
	private List<String> readFileAsList(String filename) {
		BufferedReader reader = null;
		List<String> list = new ArrayList<String>();
		try {
			String line = null;
			reader = new BufferedReader(new FileReader(filename));
			while( (line = reader.readLine()) != null ) {
				if( line.trim().length() > 0 )
					list.add(line);
			}
			reader.close();
		}
		catch(IOException e) {
			e.printStackTrace();
		}
		return list;
	}
	
	
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("GENERIC APIS: \n");
		for( String apiName: this.genericAPINames ) {
			ServiceAPI serviceAPI = this.genericAPISHT.get(apiName);
			buffer.append(serviceAPI.toString());
		}
		buffer.append("SPECIFIC APIS: \n");
		for( String apiName: this.specificAPINames ) {
			ServiceAPI serviceAPI = this.specificAPISHT.get(apiName);
			buffer.append(serviceAPI.toString());
		}
		return buffer.toString();
	}
	
	
    private double randDouble(double min, double max) {
	    double randomNum = min + (max - min) * this.rand.nextDouble();
	    return randomNum;
	}
    
    
    private int randInt(int min, int max) {
	    int randomNum = this.rand.nextInt((max - min) + 1) + min;
	    return randomNum;
	}
	
	
}




