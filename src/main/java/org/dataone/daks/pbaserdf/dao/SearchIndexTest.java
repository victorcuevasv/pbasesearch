package org.dataone.daks.pbaserdf.dao;


public class SearchIndexTest {
	
	
	SearchIndex searchIndex;
	
	
	public SearchIndexTest(String directory) {
		this.searchIndex = SearchIndex.getInstance();
		this.searchIndex.init(directory);
	}
	
	
    public static void main(String args[]) {
        //String directory = "C:\\devp\\apache-tomcat-7.0.50\\bin\\" + args[0];
        SearchIndexTest test = new SearchIndexTest(args[0]);
        System.out.println("Executing SearchIndexTest with directory: " + args[0]);
        //test.testGlobalIndex(args[1]);
        test.testWorkflowIndex(args[1], args[2]);
    }
    
    
    private void testGlobalIndex(String term) {
    	String value = this.searchIndex.get(term);
    	System.out.println("Term: " + term);
    	System.out.println(value);
    }
    
    
    private void testWorkflowIndex(String wfID, String term) {
    	String value = this.searchIndex.get(wfID + "_" + term);
    	System.out.println("wfID: " + wfID + " term: " + term);
    	System.out.println(value);
    }
    
	
}


