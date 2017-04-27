package main;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class NoraControl {
	 public static void main(String[] args) throws Exception {
		 	//this is where an intermediate file will be held.
	        String crawlStorageFolder = "./crawl/root";
	        //use 7 threads
	        int numberOfCrawlers = 7;

	        CrawlConfig config = new CrawlConfig();
	        config.setCrawlStorageFolder(crawlStorageFolder);
	        //The crawler will not exceed 2 sublevels of page crawling
	        //config.setMaxDepthOfCrawling(5);
	        //.3 second crawl delay on each outgoing link
	        config.setPolitenessDelay(300);
	        //set name of crawler to be displayed
	        config.setUserAgentString("nora (https://www.versellie.com)");

	        //make pageFetcher and robotstxtConfig
	        PageFetcher pageFetcher = new PageFetcher(config);
	        RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
	        
	        //setup identifiers for robots
	        robotstxtConfig.setUserAgentName("nora (https://www.versellie.com)");
	        
	        
	        RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
	        CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);

	        //this is where the crawler will start add as many seeds as you want
	        controller.addSeed("https://botw.org");
	       
	        
	        controller.start(NoraCrawler.class, numberOfCrawlers);
	    }
}
