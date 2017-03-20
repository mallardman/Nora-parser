package main;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

public class NoraIndexer {
	static void index(HashMap<String,Integer> data,String url){
		String noraDB = "/home/marco/parser/nora";
		//System.out.println(noraDB);
		Connection noraConn = null;
		Statement noraState = null;
		Statement hashState = null;
		String hashGetterSQL = "SELECT * FROM word_master";
		HashMap<String,Integer> wordIds = new HashMap<String,Integer>(); 

		DatabaseMetaData meta;
		int urlID;
		while(true){
			try{
				Class.forName("org.sqlite.JDBC");
				noraConn = DriverManager.getConnection("jdbc:sqlite:" + noraDB);
				//System.out.println("Connection established");
				//noraConn.setAutoCommit(false);
				//move into hash
				hashState = noraConn.createStatement();
				ResultSet hashWordsSet = 
						hashState.executeQuery(hashGetterSQL);
				while(hashWordsSet.next()){
					wordIds.put(hashWordsSet.getString("word"),
							hashWordsSet.getInt("word_id"));
				}
				hashState.close();
				hashWordsSet.close();
				break;
			}
			
			catch(Exception exe){	
				//System.out.println("Failed to open.");
			}
		}
		
		//System.out.println("Connection established");
			///////////////////////////
			for(String key:data.keySet()){
				if(key.length() <= 2) continue;
				
				while(true){
					//System.out.println(key);
				//while(true){
					try{
						//System.out.println(key);		
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
							String noraInsertId = "INSERT OR REPLACE INTO master_id (url) VALUES (\"" + url +"\")";
							noraState = noraConn.createStatement();
							noraState.execute(noraInsertId);
							noraState.close();
							noraStatePrepId = noraConn.prepareStatement(idExists);
							noraStatePrepId.setString(1, url);
							idResult = noraStatePrepId.executeQuery();
							
						}
						//get the url's id
						urlID = idResult.getInt("id");
						//close statements and result sets
						idResult.close();
						noraStatePrepId.close();
						int wordKey;
						//two character words are discarded
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
							ResultSet wordIdRS = noraState.executeQuery(getWordIdSQL);
							//doesn't exist
							if(!wordIdRS.next()){
								wordIdRS.close();
								noraState.close();
								noraState = noraConn.createStatement();
								//System.out.println("word added to ID");
								String insertWordIdSQL=
										"INSERT OR REPLACE INTO word_master (word) VALUES (\"" + key + "\")" ; 
								noraState.execute(insertWordIdSQL);
								wordIdRS = noraState.executeQuery(getWordIdSQL);
				
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
							String noraCreateSQL = "CREATE TABLE wc_" + wordKey + "(\"id\" bigint UNIQUE,\"count\" int)";
							noraState.execute(noraCreateSQL);
						}
					
						else{
							doesTableResult.close();
						}
						//System.out.println("Created table or disregarded it");
						
						noraState.close();
						//to do
						//System.out.println("Insert into word table");
						String noraInsertSQL = "INSERT OR REPLACE INTO wc_" + wordKey + " VALUES (?,?) "
								;
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
						noraPreparedStatement.execute();
						
						//noraConn.commit();
						break;
					}
					
					catch(Exception e){
						if(e instanceof NullPointerException){
							break;
						}
						
						//e.printStackTrace(System.out);
						//System.out.println("failed");
					}	
				}		
			}
			
			
			try{
				noraConn.close();
			}
			
			catch(Exception exe){	
				
			}
			
	}
}
	
