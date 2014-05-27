

Generate the data


mvn exec:java -Dexec.mainClass="org.dataone.daks.pbasesearch.DatasetGenerator" -Dexec.args="wfs.txt wfs apis genericapis.txt specificapis.txt"


Create the RDF database from the JSON files


mvn exec:java -Dexec.mainClass="org.dataone.daks.pbasesearch.DigraphJSONtoRDF" -Dexec.args="wfs wfs.txt numtraces.txt"     


Test the RDF database

mvn exec:java -Dexec.mainClass="org.dataone.daks.pbaserdf.dao.LDBDAOTest" -Dexec.args="searchgraphs"


Create the index

mvn exec:java -Dexec.mainClass="org.dataone.daks.pbasesearch.CreateIndexFromJSON" -Dexec.args="wfs wfs.txt numtraces.txt apis genericapis.txt specificapis.txt searchgraphs"     


Test the index:

mvn exec:java -Dexec.mainClass="org.dataone.daks.pbaserdf.dao.SearchIndexTest" -Dexec.args="searchindexdb ancestry"



