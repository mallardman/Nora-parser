package main;
import java.util.regex.Pattern;
import edu.uci.ics.crawler4j.crawler.*;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

public class NoraCrawler extends WebCrawler {
	//regular expression which ensures that attachments are not parsed
	private final static Pattern FILTERS = Pattern.compile(".*(\\.(css|js|gif|jpg"
            + "|png|mp3|mp3|zip|gz))$");

//determines if a given url's webpage should be requested and parsed
//returns true if if criteria met
@Override
	public boolean shouldVisit(Page referringPage, WebURL url) {
		//lower case
		String href = url.getURL().toLowerCase();
		//if it is not an attachment and is a link to a page on versellie.com, parse it
		return !FILTERS.matcher(href).matches();// && href.startsWith("https://www.versellie.com/");
	}

/**
* This function is called when a page is fetched and ready
* to be processed by your program.
*/
@Override
	public void visit(Page page) {
		String url = page.getWebURL().getURL();
		long start = System.nanoTime();
		System.out.println("URL: " + url + ": starting");
		
		if (page.getParseData() instanceof HtmlParseData) {
			//get the HTML contents of the page
			HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
			String html = htmlParseData.getHtml();
			//pass it to the parser
			NoraParser engineParser = new NoraParser(html,url.toLowerCase()); 
			engineParser.parse();
			//System.out.println("URL: " + url + ": parsed");
			
		}
		System.out.println("URL: " + url + ": finished.  " + ((System.nanoTime() - start)/1000000000) + "s");
	}
}

