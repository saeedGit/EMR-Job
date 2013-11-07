import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text(); // pairOf<cid,sid> ? 
    private String line = "";
    
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        							// pairOf<c,s> value? 
    	
    	/*
    	 * reference : http://www.javacodegeeks.com/2013/08/writing-a-hadoop-mapreduce-task-in-java.html
    	 * 
    	 * NOTE:
    	 * "Although the input to a MapReduce job may consist of multiple input files (constructed by a
    	 *  combination of file globs, filters, and plain paths), all of the input is interpreted by a single
    	 *   InputFormat and a single Mapper".
    	 *   
    	 * 1- try with one file only in the dir
    	 * 2- then 
    	 * fileinput format reference for reading (multiple+gz) files:
    	 * https://www.inkling.com/read/hadoop-definitive-guide-tom-white-3rd/chapter-7/input-formats
    	 * */
    	line = value.toString();
        while (line != null){
        	if (line.length() != 0){
        		//grab a json object from file
        		if (line.charAt(line.length() - 1) == '}' ){  
        			JSONParser parser = new JSONParser();
        			try{
						Object obj = parser.parse(line);
						JSONObject object = (JSONObject) obj;
						String event = (String) object.get("TMEvent"); 
						String campaign = "";
						String supplier = ""; 
						
						if (event.contentEquals("13")){
							// it;s a click
							
							// get campaign 
							if (object.get("campaign").getClass().toString().endsWith("Long")){
								Long campaignl = (Long) object.get("campaign");
								campaign = String.valueOf(campaignl);
							}else
								campaign = (String) object.get("campaign");
							// get supplier 
							if (object.get("supplier").getClass().toString().endsWith("Long")){
								Long supplierl = (Long) object.get("campaign");
								supplier = String.valueOf(supplierl);
							}else
								supplier = (String) object.get("supplier");
							
							// System.out.println( ": Event: "+ event + " campaign: " + campaign + " Supplier:" + supplier);
							
							// merge cid,sid into one value in order to perform GROUP BYE
							String val = campaign + "," + supplier; // set campaign+supplier OR <campaign,supplier>
							word.set(val);
							String stringWord = word.toString().toLowerCase();
							// context.write(pairOf<cid,sid>, one); ??
							// google for group by in hadoop 
							context.write(new Text(stringWord), one);
						}	
					}catch(ParseException pe){
						System.out.println("position: " + pe.getPosition());
						System.out.println(pe);
					}
        		}
        	}
        }
            
    }
    
}
