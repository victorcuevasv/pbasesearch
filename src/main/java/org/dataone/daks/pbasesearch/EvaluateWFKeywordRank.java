package org.dataone.daks.pbasesearch;

import java.util.Hashtable;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.dataone.daks.pbaserdf.dao.*;
import org.json.*;

import com.gs.collections.api.block.function.Function;
import com.gs.collections.api.list.*;
import com.gs.collections.api.tuple.Pair;
import com.gs.collections.impl.list.mutable.FastList;


public class EvaluateWFKeywordRank {
	
	
	private String DBNAME = null;
	private String INDEXDBNAME = null;

	
	SearchIndex searchIndex;
	TDBDAO dao;
	
	
	public EvaluateWFKeywordRank(String rdfDBDirectory) {
		DBNAME = rdfDBDirectory;
		INDEXDBNAME = rdfDBDirectory + "indexdb";
		this.dao = TDBDAO.getInstance();
		this.dao.init(DBNAME);
		this.searchIndex = SearchIndex.getInstance();
		this.searchIndex.init(INDEXDBNAME);
	}
	
	
	public static void main(String[] args) {
		EvaluateWFKeywordRank evaluator = new EvaluateWFKeywordRank(args[0]);
		List<String> queryList = new ArrayList<String>();
		for( int i = 1; i < args.length; i++ )
			queryList.add(args[i]);
		String jsonArrayStr = evaluator.getWfIdsKeywordRanked(queryList);
		System.out.println(jsonArrayStr);
	}
	
	
	private String getWfIdsKeywordRanked(List<String> queryList) {
		String baseJSONArrayStr = this.dao.getWfIDs();
		String retVal = null;
		try {
			JSONArray baseJSONArray = new JSONArray(baseJSONArrayStr);
			Hashtable<String, Integer> wfCountHT = this.generateMatchesCountHT(queryList);
			MutableList<String> idsList = FastList.newList();
			MutableList<Integer> countList = FastList.newList();
			for( int i = 0; i < baseJSONArray.length(); i++ ) {
				String wfID = baseJSONArray.getString(i);
				idsList.add(wfID);
				Integer matchCount = wfCountHT.get(wfID);
				if( matchCount != null )
					countList.add(matchCount);
				else
					countList.add(0);
			}
			MutableList<Pair<String, Integer>> pairs = idsList.zip(countList); 
			MutableList<Pair<String, Integer>> sortedPairs = pairs.toSortedListBy(
					new Function<Pair<String, Integer>, Integer>() { 
						public Integer valueOf(Pair<String, Integer> pair) { 
							return pair.getTwo();
						} 
					} );
			MutableList<Pair<String, Integer>> sortedPairsRev = sortedPairs.reverseThis();
			System.out.println(sortedPairsRev);
			MutableList<String> sortedIdsList = sortedPairsRev.collect(
					new Function<Pair<String, Integer>, String>() { 
						public String valueOf(Pair<String, Integer> pair) { 
							return pair.getOne();
						} 
					} );
			System.out.println(sortedIdsList);
			JSONArray sortedIdsArray = new JSONArray();
			for( String wfID : sortedIdsList )
				sortedIdsArray.put(wfID);
			retVal = sortedIdsArray.toString();
		}
		catch (JSONException e) {
			e.printStackTrace();
		}
		return retVal;
	}
	
	
	private Hashtable<String, Integer> generateMatchesCountHT(List<String> queryList) {
		Hashtable<String, Integer> wfCountHT = new Hashtable<String, Integer>(); 
		for( String word : queryList ) {
			String indexedStr = this.searchIndex.get(word);
			StringTokenizer tokenizer = new StringTokenizer(indexedStr);
			while( tokenizer.hasMoreTokens() ) {
				String token = tokenizer.nextToken();
				String wfID = token.substring(0, token.indexOf(':'));
				String countStr = token.substring(token.indexOf(':')+1, token.length());
				Integer oldCount = wfCountHT.get(wfID);
				if( oldCount == null )
					wfCountHT.put(wfID, Integer.parseInt(countStr));
				else
					wfCountHT.put(wfID, oldCount + Integer.parseInt(countStr));
			}
		}
		return wfCountHT;
	}
	
	
}






