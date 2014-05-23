package org.dataone.daks.pbasesearch;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.util.Version;

import java.util.List;
import java.util.ArrayList;


public class LuceneAnalyzer {

	
	Analyzer analyzer;
	
	
	public LuceneAnalyzer() {
		this.analyzer = new StandardAnalyzer(Version.LUCENE_30);
	}
	
	
	public List<String> parse(String text) {
		List<String> tokens = null;
		try {
			tokens = displayTokens(this.analyzer, text);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		return tokens;
	}
	
	
	private List<String> displayTokens(Analyzer analyzer, String text) throws IOException {
		return displayTokens(analyzer.tokenStream("contents", new StringReader(text)));
	}

	
	private List<String> displayTokens(TokenStream stream) throws IOException {
		List<String> tokens = new ArrayList<String>();
		TermAttribute term = stream.addAttribute(TermAttribute.class);
		while(stream.incrementToken()) {
			tokens.add(term.term());
		}
		return tokens;
	}
	
	
}




