
package org.dataone.daks.pbasesearch;


import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Hashtable;
import java.util.Set;

import org.json.*;

import org.dataone.daks.pbaserdf.dao.*;


public class CreateIndexFromJSON {
	
	
	private static final String INDEXDB_DIR = "searchindexdb";
	
	private Hashtable<String, Hashtable<String, Integer>> globalIndex;
	private LuceneAnalyzer analyzer;
	private APISCatalog catalog;
	private SearchIndex searchIndex;
	
	
	public CreateIndexFromJSON(String apisFolder, String genericApisFile, String specificApisFile) {
		this.analyzer = new LuceneAnalyzer();
		this.globalIndex = new Hashtable<String, Hashtable<String, Integer>>();
		this.catalog = new APISCatalog(apisFolder, genericApisFile, specificApisFile);
		this.searchIndex = SearchIndex.getInstance();
		this.searchIndex.init(INDEXDB_DIR);
	}
	
	
	public static void main(String args[]) {
		if( args.length != 6 ) {
			System.out.println("Usage: java org.dataone.daks.pbasesearch.CreateIndexFromJSON <json files folder> <wf ids file> <n traces file> " +      
					"<apis folder> <generic apis file> <specific apis file>");      
			System.exit(0);
		}
		CreateIndexFromJSON indexer = new CreateIndexFromJSON(args[3], args[4], args[5]);
		List<String> wfNamesList = indexer.createWFNamesList(args[1]);
		HashMap<String, Integer> numTracesHT = indexer.createNumTracesHT(args[2]);
		String folder = args[0];
		for(String wfName: wfNamesList) {
			String wfJSONStr = indexer.readFile(folder + "/" + wfName + ".json");
			indexer.processWFJSONString(wfJSONStr, wfName);
			int nTraces = numTracesHT.get(wfName);
			for( int i = 1; i <= nTraces; i++ ) {
				String traceJSONStr = indexer.readFile(folder + "/" + wfName + "trace" + i + ".json");
				indexer.processTraceJSONString(traceJSONStr, wfName, i);
			}
		}
		indexer.storeGlobalIndex();
		indexer.closeSearchIndex();
	}

	
	private List<String> createWFNamesList(String wfNamesFile) {
		String wfNamesText = readFile(wfNamesFile);
		List<String> wfNamesList = new ArrayList<String>();
		StringTokenizer tokenizer = new StringTokenizer(wfNamesText);
		while( tokenizer.hasMoreTokens() ) {
			String token = tokenizer.nextToken();
			wfNamesList.add(token);
		}
		return wfNamesList;
	}
	
	
	private HashMap<String, Integer> createNumTracesHT(String numTracesFile) {
		String numTracesText = readFile(numTracesFile);
		HashMap<String, Integer> numTracesHT = new HashMap<String, Integer>();
		StringTokenizer tokenizer = new StringTokenizer(numTracesText);
		while( tokenizer.hasMoreTokens() ) {
			String wfId = tokenizer.nextToken();
			String nTracesStr = tokenizer.nextToken();
			int nTraces = Integer.parseInt(nTracesStr);
			numTracesHT.put(wfId, nTraces);
		}
		return numTracesHT;
	}
	
	
	public void processWFJSONString(String jsonStr, String wfID) {
		try {
			JSONObject graphObj = new JSONObject(jsonStr);
			JSONArray nodesArray = graphObj.getJSONArray("nodes");
			//Iterate over nodes to index process entities
			for( int i = 0; i < nodesArray.length(); i++ ) {
				JSONObject nodeObj = nodesArray.getJSONObject(i);
				String nodeId = nodeObj.getString("nodeId");
				//Generate service object property for processes bound to services
				if( nodeObj.has("service") ) {
					String fullServiceName = nodeObj.getString("service");
					String serviceDesc = this.catalog.getServiceDesc(fullServiceName);
					List<String> tokensList = this.analyzer.parse(serviceDesc);
					for( String token : tokensList ) {
						Hashtable<String, Integer> globalEntry = this.globalIndex.get(token);
						if( globalEntry == null ) {
							Hashtable<String, Integer> newEntry = new Hashtable<String, Integer>();
							newEntry.put(wfID, 1);
							this.globalIndex.put(token, newEntry);
						}
						else if( globalEntry != null ) {
							Integer oldCount = globalEntry.get(wfID);
							if( oldCount == null )
								globalEntry.put(wfID, 1);
							else
								globalEntry.put(wfID, oldCount+1);
						}
						this.createWFIndexEntry(wfID, token, nodeId);
					}
				}
			}
		}
		catch(JSONException e) {
			e.printStackTrace();
		}
	}
	
	
	public void processTraceJSONString(String jsonStr, String wfID, int traceNumber) {
		try {
			JSONObject graphObj = new JSONObject(jsonStr);
			JSONArray nodesArray = graphObj.getJSONArray("nodes");
			//Iterate over nodes to generate process execution entities
			for( int i = 0; i < nodesArray.length(); i++ ) {
				JSONObject nodeObj = nodesArray.getJSONObject(i);
				String nodeId = nodeObj.getString("nodeId");
				//Check if the node is an activity or a data item
				if( !nodeId.contains("_") ) { //activity
					if( nodeObj.has("service") ) {
						String fullServiceName = nodeObj.getString("service");
						String serviceDesc = this.catalog.getServiceDesc(fullServiceName);
						List<String> tokensList = this.analyzer.parse(serviceDesc);
						for( String token : tokensList ) {
							this.createTraceIndexEntry(wfID, traceNumber, token, nodeId);
						}
					}
				}
				else { //data
					; //currently data have no descriptions and are therefore not indexed
				}
			}
		}
		catch(JSONException e) {
			e.printStackTrace();
		}
	}
	
	
	private void createWFIndexEntry(String wfID, String token, String nodeId) {
		String oldVal = this.searchIndex.get(wfID + "_" + token);
		String newVal = null;
		if( oldVal == null )
			newVal = nodeId;
		else
			newVal = oldVal + " " + nodeId;
		this.searchIndex.put(wfID + "_" + token, newVal);
	}
	
	
	private void createTraceIndexEntry(String wfID, int traceNumber, String token, String nodeId) {
		String oldVal = this.searchIndex.get(wfID + "trace" + traceNumber + "_" + token);
		String newVal = null;
		if( oldVal == null )
			newVal = nodeId;
		else
			newVal = oldVal + " " + nodeId;
		this.searchIndex.put(wfID + "trace" + traceNumber + "_" + token, nodeId);
	}
	
	
	private void storeGlobalIndex() {
		Set<String> tokens = this.globalIndex.keySet();
		for( String token : tokens ) {
			Hashtable<String, Integer> ht = this.globalIndex.get(token);
			Set<String> wfIDs = ht.keySet();
			StringBuffer buffer = new StringBuffer();
			for( String wfID : wfIDs ) {
				int count = ht.get(wfID);
				buffer.append(wfID + ":" + count + " ");
			}
			String value = buffer.toString();
			this.searchIndex.put(token, value);
			//System.out.println(token + " = " + value);
		}
	}
	
	
	private void closeSearchIndex() {
		this.searchIndex.shutdown();
	}
	
	
	private String readFile(String filename) {
		StringBuilder builder = null;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(filename));
			String line = null;
			builder = new StringBuilder();
			String NEWLINE = System.getProperty("line.separator");
			while( (line = reader.readLine()) != null ) {
				builder.append(line + NEWLINE);
			}
			reader.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		return builder.toString();
	}
	
	
}


