package main;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.jsoup.*;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


public class NoraParser {
	private String mainHTML;
	private String url;
	Document mainDoc = null;
	HashMap<String,Integer> wordCount = 
			new HashMap<String,Integer>();
	public NoraParser(String in,String urlIn){
		this.mainHTML = in;
		this.url = urlIn;
		this.mainDoc = Jsoup.parse(mainHTML);
	}
	
	public void parse(){
		recParse(mainDoc);
		for(String i:wordCount.keySet()){
			//System.out.println(i + ": " + wordCount.get(i));
		}
		NoraIndexer.index(wordCount, url);
	}
	
	public void recParse(Element base){
		//Elements childSections = null;
		for(Element i : base.children()){
			String lower = i.tag().getName().toLowerCase();
			if(!lower.equals("script") || !lower.equals("del")
					|| !lower.equals("embed") || !lower.equals("video")
					|| !lower.equals("source")|| !lower.equals("audio")
					|| !lower.equals("applet")){
				recParse(i);
			}
		}
		if (!base.hasText()) return;
		String remainingWords = base.ownText();
		String delimiters = "      ][<>_|$%#@^&–*\\,?“”}{/!\t\n()=+:-\";'.";
		StringTokenizer countTokenizer = 
				new StringTokenizer(remainingWords,delimiters, false);
		while(countTokenizer.hasMoreTokens()){
			String tempWord = countTokenizer.nextToken().toLowerCase();
			if(wordCount.containsKey(tempWord)){
				wordCount.put(tempWord.toLowerCase(), wordCount.get(tempWord) + 1);
			}
			
			else{
				wordCount.put(tempWord,1);
			}
		}
		
		
	}
}
