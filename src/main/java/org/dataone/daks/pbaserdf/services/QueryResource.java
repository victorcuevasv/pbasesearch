
package org.dataone.daks.pbaserdf.services;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.dataone.daks.pbaserdf.dao.TDBDAO;

/** Example resource class hosted at the URI path "/queryresource"
 */
@Path("/queryresource")
public class QueryResource {
    
    /** Method processing HTTP GET requests, producing "text/plain" MIME media
     * type.
     * @return String that will be send back as a response of type "text/plain".
     */
    @GET 
    @Produces("text/plain")
    public String getIt(@QueryParam("dbname") String dbname, @QueryParam("query") String query) {
    	TDBDAO dao = TDBDAO.getInstance();
    	dao.init(dbname);
    	String retVal = null;
    	try {
    		System.out.println(query);
    		retVal = dao.executeQuery(query);
    	}
    	catch(Exception e) {
    		e.printStackTrace();
    	}
    	return retVal;
    }
}


