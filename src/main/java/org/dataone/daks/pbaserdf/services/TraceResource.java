
package org.dataone.daks.pbaserdf.services;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.dataone.daks.pbaserdf.dao.TDBDAO;
import org.dataone.daks.pbasesearch.EvaluateObjectRank;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/** Example resource class hosted at the URI path "/traceresource"
 */
@Path("/traceresource")
public class TraceResource {
    
    /** Method processing HTTP GET requests, producing "text/plain" MIME media
     * type.
     * @return String that will be send back as a response of type "text/plain".
     */
    @GET 
    @Produces("text/plain")
    public String getIt(@QueryParam("dbname") String dbname, @QueryParam("wfid") String wfid,
    		@QueryParam("traceid") String traceid, @QueryParam("keywords") String keywords,
    		@QueryParam("andsemantics") String andsemantics, @QueryParam("onlytable") String onlytable) {
    	String retVal = null;
    	TDBDAO dao = TDBDAO.getInstance();
    	dao.init(dbname);
    	if( keywords == null ) {
    		retVal = dao.getTrace(wfid, traceid);
		}
    	else {
    		try {
    			EvaluateObjectRank evaluator = new EvaluateObjectRank(dbname);
    			StringTokenizer tokenizer = new StringTokenizer(keywords);
    			List<String> keywordList = new ArrayList<String>();
    			while( tokenizer.hasMoreTokens() )
    				keywordList.add( tokenizer.nextToken() );
    			boolean andSemantics = false;
    			boolean onlyTable = false;
    			if( andsemantics != null && andsemantics.equalsIgnoreCase("true") )
    				andSemantics = true;
    			if( onlytable != null && onlytable.equalsIgnoreCase("true") )
    				onlyTable = true;
    			retVal = evaluator.processKeywordQuery(wfid, keywordList, andSemantics, onlyTable, traceid);
    		}
    		catch(Exception e) {
    			e.printStackTrace();
    		}
    	}
    	return retVal;
    }
    
    
	private String getFileContents(String filename) {
		BufferedReader reader = null;
		StringBuffer buff = new StringBuffer();
		String line = null;
		try {
			reader = new BufferedReader(new FileReader(filename));
			while( (line = reader.readLine()) != null ) {
				buff.append(line);
			}
			reader.close();
		}
		catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		return buff.toString();
	}
	
	
}


