package org.dataone.daks.pbasesearch;

import java.util.Hashtable;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.StringTokenizer;

import org.dataone.daks.pbase.treecover.Digraph;
import org.dataone.daks.pbaserdf.dao.*;
import org.json.*;


public class EvaluateWFObjectRank {
	
	
	private String DBNAME = null;
	private String INDEXDBNAME = null;

	
	SearchIndex searchIndex;
	TDBDAO dao;
	
	
	public EvaluateWFObjectRank(String rdfDBDirectory) {
		DBNAME = rdfDBDirectory;
		INDEXDBNAME = rdfDBDirectory + "indexdb";
		this.dao = TDBDAO.getInstance();
		this.dao.init(DBNAME);
		this.searchIndex = SearchIndex.getInstance();
		this.searchIndex.init(INDEXDBNAME);
	}
	
	
	public static void main(String[] args) {
		EvaluateWFObjectRank evaluator = new EvaluateWFObjectRank(args[0]);
		String wfID = args[1];
		List<String> queryList = new ArrayList<String>();
		for( int i = 2; i < args.length; i++ )
			queryList.add(args[i]);
		Digraph digraph = new Digraph();
		JSONObject jsonObj = new JSONObject();
		evaluator.fillDigraphAndTopSortedJSONObj(wfID, digraph, jsonObj);
		Hashtable<String, Double> objectRankHT = null;
		Hashtable<String, Double> tempObjectRankHT = null;
		for( int i = 0; i < queryList.size(); i++ ) {
			String word = queryList.get(i);
			if( i == 0 ) {
				objectRankHT = evaluator.evaluateObjectRank(wfID, digraph, jsonObj, word);     
				System.out.println("First objectRank values:");
				evaluator.printObjectRankHT(objectRankHT);
			}
			if( i > 0 ) {
				tempObjectRankHT = evaluator.evaluateObjectRank(wfID, digraph, jsonObj, word);
				System.out.println("Next objectRank values:");
				evaluator.printObjectRankHT(tempObjectRankHT);
				Set<String> htKeys = tempObjectRankHT.keySet();
				for( String key : htKeys ) {
					double val = objectRankHT.get(key);
					double tempVal = tempObjectRankHT.get(key);
					objectRankHT.put(key, val * tempVal);
				}
				System.out.println("Combined objectRank values:");
				evaluator.printObjectRankHT(objectRankHT);
			}
		}
	}
	
	
	//IMPORTANT: digraph and jsonObj are expected as just created objects and empty
	private void fillDigraphAndTopSortedJSONObj(String wfID, Digraph digraph, JSONObject jsonObj) {
		String wfJSONStr = this.dao.getWorkflowReachEncoding(wfID);
		JSONObject wfObj = null;
		try {
			wfObj = new JSONObject(wfJSONStr);
			JSONArray edgesArray = wfObj.getJSONArray("edges");
			JSONArray nodesArray = wfObj.getJSONArray("nodes");
			Hashtable<String, JSONObject> nodesHT = new Hashtable<String, JSONObject>();
			for( int i = 0; i < nodesArray.length(); i++ ) {
				JSONObject nodeObj = nodesArray.getJSONObject(i);
				nodesHT.put(nodeObj.getString("nodeId"), nodeObj);
			}
			Digraph topSortDigraph = new Digraph();
        	for( int i = 0; i < edgesArray.length(); i++ ) {
        		JSONObject edgeObj = edgesArray.getJSONObject(i);
        		digraph.addEdge(edgeObj.getString("startNodeId"), edgeObj.getString("endNodeId"));
        		topSortDigraph.addEdge(edgeObj.getString("startNodeId"), edgeObj.getString("endNodeId"));
        	}
        	JSONArray topSortedNodesArray = new JSONArray();
        	for( String nodeId : topSortDigraph.topSort() ) {
        		JSONObject nodeObj = nodesHT.get(nodeId);
        		topSortedNodesArray.put(nodeObj);
        	}
    		jsonObj.put("nodes", topSortedNodesArray);
    		jsonObj.put("edges", edgesArray);
		}
		catch (JSONException e) {
			e.printStackTrace();
		}
	}
	
	
	private Hashtable<String, Double> evaluateObjectRank(String wfID, Digraph digraph, JSONObject jsonObj,
			String word) {
		double d = 0.2;
		double transferRate = 0.2;
		Hashtable<String, Double> objectRankHT = new Hashtable<String, Double>();
		String matchNodesStr = this.searchIndex.get(wfID + "_" + word);
		List<String> matchNodesList = new ArrayList<String>();
		StringTokenizer tokenizer = new StringTokenizer(matchNodesStr);
		while( tokenizer.hasMoreTokens() ) {
			String token = tokenizer.nextToken();
			matchNodesList.add(token);
			System.out.println("Match node: " + token);
		}
		JSONArray topSortedNodesArray = null;
		double base = 1.0/ digraph.nVertices();
		double val = (1.0-d) / matchNodesList.size();
		System.out.println("val: " + val);
		System.out.println("base: " + base);
		try {
			topSortedNodesArray = jsonObj.getJSONArray("nodes");
			for( int i = 0; i < topSortedNodesArray.length(); i++ ) {
				JSONObject nodeObj = topSortedNodesArray.getJSONObject(i);
				String nodeId = nodeObj.getString("nodeId");
				if( matchNodesList.contains(nodeId) )
					objectRankHT.put(nodeId, d * base + val);
				else
					objectRankHT.put(nodeId, d * base);
			}
			for( int i = 0; i < topSortedNodesArray.length(); i++ ) {
				JSONObject nodeObj = topSortedNodesArray.getJSONObject(i);
				String nodeId = nodeObj.getString("nodeId");
				//System.out.println(nodeId);
				double nodeOR = objectRankHT.get(nodeId);
				List<String> adjList = digraph.getAdjList(nodeId);
				double alpha = 0.0;
				if( adjList.size() > 0 )
					alpha = transferRate / adjList.size();
				for( String adjNode : adjList ) {
					double adjNodeOR = objectRankHT.get(adjNode);
					objectRankHT.put(adjNode, adjNodeOR + nodeOR * alpha );
				}
			}
		}
		catch (JSONException e) {
			e.printStackTrace();
		}
		return objectRankHT;
	}
	
	
	private void printObjectRankHT(Hashtable<String, Double> objectRankHT) {
		Set<String> keys = objectRankHT.keySet();
		for( String key : keys ) {
			double objectRankVal = objectRankHT.get(key);
			String objectRankStr = String.format("%.3f", objectRankVal);
			System.out.println("Node " + key + ":" + objectRankStr);
		}
	}
	
	
}






