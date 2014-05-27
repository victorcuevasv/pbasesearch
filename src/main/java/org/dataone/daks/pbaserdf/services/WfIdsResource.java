
package org.dataone.daks.pbaserdf.services;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.dataone.daks.pbaserdf.dao.TDBDAO;
import org.dataone.daks.pbasesearch.EvaluateWFKeywordRank;

/** Example resource class hosted at the URI path "/wfidsresource"
 */
@Path("/wfidsresource")
public class WfIdsResource {
    
    /** Method processing HTTP GET requests, producing "text/plain" MIME media
     * type.
     * @return String that will be send back as a response of type "text/plain".
     */
    @GET 
    @Produces("text/plain")
    public String getIt(@QueryParam("dbname") String dbname, 
    		@QueryParam("keywords") String keywords) {
    	String retVal = null;
    	try {
    		if( keywords == null ) {
    			TDBDAO dao = TDBDAO.getInstance();
    	    	dao.init(dbname);
    			retVal = dao.getWfIDs();
    		}
    		else {
    			EvaluateWFKeywordRank evaluator = new EvaluateWFKeywordRank(dbname);
    			StringTokenizer tokenizer = new StringTokenizer(keywords);
    			List<String> keywordList = new ArrayList<String>();
    			while( tokenizer.hasMoreTokens() )
    				keywordList.add( tokenizer.nextToken() );
    			retVal = evaluator.getWfIdsKeywordRanked(keywordList);
    		}
    	}
    	catch(Exception e) {
    		e.printStackTrace();
    	}
    	return retVal;
    }
    
    
}




