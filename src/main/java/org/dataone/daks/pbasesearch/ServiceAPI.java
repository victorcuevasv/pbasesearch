package org.dataone.daks.pbasesearch;

import java.util.List;
import java.util.Hashtable;
import java.util.ArrayList;
import java.util.Iterator;

import org.dom4j.*;
import org.dom4j.io.SAXReader;


public class ServiceAPI {
	
	
	private List<String> serviceNamesList;
	private Hashtable<String, String> serviceDescriptionsHT;
	private String apiName;
	
	
	public ServiceAPI(String xmlFile) {
		this.serviceNamesList = new ArrayList<String>();
		this.serviceDescriptionsHT = new Hashtable<String, String>();
		String filePrefix = xmlFile.substring(0, xmlFile.length()-4);
		this.apiName = filePrefix;
		this.init(xmlFile);
	}
	
	
	public String getApiName() {
		return this.apiName;
	}
	
	
	public String getServiceDesc(String serviceName) {
		return this.serviceDescriptionsHT.get(serviceName);
	}
	
	
	public int getNServices() {
		return this.serviceNamesList.size();
	}
	
	
	public String getServiceNameAt(int position) {
		return this.serviceNamesList.get(position);
	}

	
	private void init(String xmlFileName) {
		Document document = this.getDocument(xmlFileName);
		Element root = document.getRootElement();
		for ( Iterator<Element> i = root.elementIterator("service"); i.hasNext(); ) {
			Element serviceElem = i.next();
			Element elemName = serviceElem.element("name");
			String name = elemName.getText();
			Element elemDesc = serviceElem.element("description");
			String desc = elemDesc.getText();
			this.serviceDescriptionsHT.put(name, desc);
			this.serviceNamesList.add(name);
		}
		
	}
	
	
	private Document getDocument(String xmlFileName) {
		Document document = null;
		SAXReader reader = new SAXReader();
		try {
			document = reader.read(xmlFileName);
		}
		catch (DocumentException e) {
			e.printStackTrace();
		}
		return document;
	}
	
	
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		String newLine = System.getProperty("line.separator");
		for( String name : this.serviceNamesList ) {
			String desc = this.serviceDescriptionsHT.get(name);
			if( desc.length() >= 20 )
				desc = desc.substring(0, 19);
			buffer.append("Name: " + name + " Desc: " + desc + newLine);
		}
		return buffer.toString();
	}
	
	
}


