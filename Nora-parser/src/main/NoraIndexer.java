package main;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class NoraIndexer {
	static void index(HashMap<String,Integer> data,String url){
		String noraDB = "";
		String driverType = null;
		String propertiesDir = "/home/marco/parser/conn.properties";
		Properties prop = new Properties();
		InputStream input = null;
		Connection noraConn = null;
		Statement noraState = null;
		Statement hashState = null;
		DatabaseMetaData meta;
		int urlID;
		String hashGetterSQL = "SELECT * FROM word_master";
		HashMap<String,Integer> wordIds = new HashMap<String,Integer>(); 
		
		//load properties file
		try{
			//open properties file and load into properties object
			input = new FileInputStream(propertiesDir);
			prop.load(input);
				
			//pull driver from properties file
			driverType = prop.getProperty("driver");
				
			//Generate connector string
			noraDB = prop.getProperty("host");
			noraDB = noraDB + "/" + prop.getProperty("database") + "?user=";				noraDB = noraDB + prop.getProperty("username") + "&password=";
			noraDB = noraDB + prop.getProperty("password");
				
			//close properties file
			input.close();
		}
			
		//on failure print stack trace
		catch(IOException ex){
			ex.printStackTrace();
		}
		
		//create connection to database
		try{
			Class.forName(driverType);
			noraConn = DriverManager.getConnection(noraDB);	
			noraConn.setAutoCommit(false);
		}
		
		//Failure to connect prints stack trace	
		catch(Exception exe){	
			exe.printStackTrace();
		}
			
			
		//try to create a statement to get a list of words 
		//which are already in the system
		try{
			hashState = noraConn.createStatement();
			ResultSet hashWordsSet = 
					hashState.executeQuery(hashGetterSQL);
			while(hashWordsSet.next()){
				wordIds.put(hashWordsSet.getString("word"),
						hashWordsSet.getInt("word_id"));
			}
			hashState.close();
			hashWordsSet.close();
		}
			

		catch(Exception exe){	
			exe.printStackTrace();
		}
		
		//get 
		for(String key:data.keySet()){
			if(key.length() <= 2) continue;
				//System.out.println(key);
				try{
					//create the statement
					noraState = noraConn.createStatement();
					//word ID	
					String idExists = "SELECT * FROM master_id WHERE url = ?";
					//check to see if the url being indexed has already been indexed.
					PreparedStatement noraStatePrepId = noraConn.prepareStatement(idExists);
					noraStatePrepId.setString(1, url);
					ResultSet idResult = noraStatePrepId.executeQuery();
					//add the url if it isn't there.
					if(!idResult.next()){
						idResult.close();
						noraStatePrepId.close();
						noraState.close();
						String noraInsertId = "INSERT IGNORE INTO master_id (url) VALUES (\"" + url +"\")";
						noraState = noraConn.createStatement();
						noraState.execute(noraInsertId);
						noraState.close();
						noraStatePrepId = noraConn.prepareStatement(idExists);
						noraStatePrepId.setString(1, url);
						idResult = noraStatePrepId.executeQuery();	
						idResult.next();
					}
					
					else{
						noraState.close();
					}
					
					//get the url's id
					urlID = idResult.getInt("id");
					//close statements and result sets
					idResult.close();
					noraStatePrepId.close();
					int wordKey;//two character words are discarded
					//if(key.length() <= 2) break;
					//System.out.println(key);
					//find word key
					//determine if word key exists
					if(wordIds.containsKey(key)){
						wordKey = wordIds.get(key);
					}
						
					else{
						String getWordIdSQL = 
								"SELECT * FROM word_master WHERE word=\"" + key + "\"";
						noraState = noraConn.createStatement();
						ResultSet wordIdRS = noraState.executeQuery(getWordIdSQL);
						//doesn't exist
						if(!wordIdRS.next()){
							wordIdRS.close();
							noraState.close();
							noraState = noraConn.createStatement();
							//System.out.println("word added to ID");
							String insertWordIdSQL=
								"INSERT IGNORE INTO word_master (word) VALUES (\"" + key + "\")" ; 
							noraState.execute(insertWordIdSQL);
							wordIdRS = noraState.executeQuery(getWordIdSQL);	
							wordIdRS.next();
						}
						
						//after both options
						wordKey = wordIdRS.getInt("word_id");
						wordIdRS.close();
						noraState.close();	
						noraState = noraConn.createStatement();
					}
					
					
					//check to see if table exists
					meta = noraConn.getMetaData();
					//check to see if the table exists
					ResultSet doesTableResult = meta.getTables(null, null , "wc_" + wordKey , null);
					if(!doesTableResult.next()){
						doesTableResult.close();
						String noraCreateSQL = "CREATE TABLE wc_" + wordKey + "(id bigint UNIQUE,count int)";
						noraState.execute(noraCreateSQL);
					}
					
					else{
						doesTableResult.close();
					}
					
					noraState.close();
					//to do
					//System.out.println("Insert into word table");
					String noraInsertSQL = "INSERT INTO wc_" + wordKey + "(id,count) VALUES (?,?) "
							+ "ON DUPLICATE KEY UPDATE count = ?";
					//System.out.println("inserted into word table");
					PreparedStatement noraPreparedStatement = 
						noraConn.prepareStatement(noraInsertSQL);
					noraPreparedStatement.setLong(1, urlID);
					//check to make integers strings
					String stupidTemp;
					try{
						Integer tempInt = Integer.parseInt(key);
						stupidTemp = tempInt.toString();
					}
					
					catch(NumberFormatException numberExe){
						stupidTemp = key;
					}
					
					noraPreparedStatement.setLong(2,data.get(stupidTemp));
					noraPreparedStatement.setLong(3,data.get(stupidTemp));
					noraPreparedStatement.execute();
					
					
				}
					
				catch(Exception e){
					e.printStackTrace(System.out);
					if(e instanceof NullPointerException){
						break;
					}
						
				}	
						
		}
			
			
		try{
			noraConn.commit();
			noraConn.close();
		}
			
		catch(Exception exe){	
			exe.printStackTrace(System.out);	
		}
			
	}
}
	
