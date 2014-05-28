
package org.dataone.daks.pbaserdf.services;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.dataone.daks.pbaserdf.dao.TDBDAO;
import org.dataone.daks.pbasesearch.EvaluateObjectRank;

/** Example resource class hosted at the URI path "/wfresource"
 */
@Path("/wfresource")
public class WfResource {
    
    /** Method processing HTTP GET requests, producing "text/plain" MIME media
     * type.
     * @return String that will be send back as a response of type "text/plain".
     */
    @GET 
    @Produces("text/plain")
    public String getIt(@QueryParam("dbname") String dbname, @QueryParam("wfid") String wfid, 
    		@QueryParam("keywords") String keywords, @QueryParam("andsemantics") String andsemantics,
    		@QueryParam("onlytable") String onlytable) {
    	String retVal = null;
    	TDBDAO dao = TDBDAO.getInstance();
    	dao.init(dbname);
    	if( keywords == null ) {
    		retVal = dao.getWorkflowReachEncoding(wfid);
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
    			retVal = evaluator.processKeywordQuery(wfid, keywordList, andSemantics, onlyTable, null);
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



