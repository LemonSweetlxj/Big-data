package comp9313.ass4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




/**
 * This project is finished by Xinjie LU on 23/10
 * 
 * The whole progress has 3 map-reduce round.
 * Because the token has been sorted, so we need to write 2 mapreduce round
 * 
 * The first MR Round is to find the RID pairs:
 * 
 *   map: get the similarity from input, for each input line, calculate the prefix lenth
 *        the output of map:  prefix 1  original line
 *                            prefix 2  original line
 *                            ………………
 *   reduce: find the number of union and the number of intersection of any two pairs
 *   		 using jaccard similarity algorithm to find the similarity
 *           put the smaller RID in the frount and regard (RID1 , RID2) as key, similarity as value
 *           write the record with similarity larger than the threshold
 *           the output of reduce:  ( RID1, RID2 )  similarity 
 *     
 */
public class SetSimJoin {
	
	
	
	public static String OUT = "output";
    public static String IN = "input";
    public static double similarity;
    public static int NumOfReducer;
    
    
    public static class RIDMapper extends Mapper<Object, Text, Text, Text>
    {		
     	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			
    		    Configuration config = context.getConfiguration();
	        double similarity = config.getDouble("similarity", 0.0);
    		    String[] allStrings = value.toString().split(" ");    		
    		    int prefixLen = allStrings.length - (int) Math.ceil((allStrings.length - 1) * similarity);		
			
		    for(int i = 1; i <= prefixLen; i++)
		    {				
				context.write(new Text(allStrings[i]), value);								
			}				
		}		
	}
    
    
    public static class RIDReducer extends Reducer<Text, Text, Text, DoubleWritable> 
    {
    	public void reduce(Text key, Iterable<Text> values, Context context)
    			throws IOException, InterruptedException 
    	{
    		//get the threshold similarity
    		//System.out.println("---");
    		Configuration conf = context.getConfiguration();
		double similarity = conf.getDouble("similarity", 0.0);
    		//put all candidates with same key in an Array list
    		//the list like [1 A B C, 21 B A F]
    		ArrayList<String> sameKey = new ArrayList<String>();
			for (Text val : values) 
			{
				sameKey.add(val.toString());
			}
						
			//compute the similarity between pairs
			for(int i = 0; i < sameKey.size();i++)
			{
				//candidate = [1,A,B,C]
				String[] candidate = sameKey.get(i).split(" ");
				String RID1 = candidate[0];
				Integer RID1Int = Integer.parseInt(RID1);
				ArrayList<String> element = new ArrayList<String>();
				//remove the row ID, only save the content
				for (int m = 1; m < candidate.length; m++)
				{
					element.add(candidate[m]);
				}
							
				for(int j = i+1; j < sameKey.size();j++)
				{					
					
					double intersection = 0;
					//basic case union [21,B,A,F]
					double union = candidate.length - 1;
					
					//pair = [21,B,A,F]
					String[] pair = sameKey.get(j).split(" ");
					String RID2 = pair[0];	
					Integer RID2Int = Integer.parseInt(RID2);
					
					for(int k = 1; k < pair.length; k ++)
					{						
						//has intersection
						if(element.contains(pair[k])) 
						{
							intersection += 1;							
						}
						else {union += 1;}						
					}
					
					//Jaccard similarity algorithm
					double calculate = intersection/union;
					
					if (calculate >= similarity)
					{
						if( RID1Int < RID2Int) 
						{
							context.write(new Text(RID1 +"," + RID2), new DoubleWritable(calculate));
						}
						else{context.write(new Text(RID2 +"," + RID1), new DoubleWritable(calculate));}
					}					
				}				
			}   		
    		}
    }
	
    
    /**
     * The second MR Round is to remove the duplicated record
     * 
     * map: rewrite the key value pair
     *      the output of map: key: RID1  value: RID2  Similarity
     * 
     * reduce:
     *      1. using an Arraylist to store the record with the same key only once
     *      2. using hashmap to put RID2 and similarity as <k,v> pair
     *      3. using treemap to sort the map
     *      4. the reduce step has automaticly sort the pair in ascending order
     *      
     */
    public static class RemoveDuMapper extends Mapper<Object, Text, IntWritable, Text> 
    {
        	private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            String [] line = value.toString().split("\t");
            String RIDs = line[0];
            String[] RIDPair = RIDs.split(",");
            int RID_1 = Integer.parseInt(RIDPair[0]);
            
            String RID_2 = RIDPair[1];
            String Sim = line[1];
            word.set(RID_2 + "\t" + Sim);
            
        	context.write(new IntWritable(RID_1), word);
        }
    }
    
   public static class RemoveDuReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

    	    private Text word = new Text();
    	
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
         	//remove the duplicate result
        		ArrayList<String> sameRID = new ArrayList<String>();
        	    for (Text val : values) 
		    {
        		    if ( ! sameRID.contains(val.toString()))
        		    {
        			    sameRID.add(val.toString());
        		    }				
		    }
        	
        	    // set RID2 and similarity as pair
        	    Map<Integer, Double> map = new HashMap<Integer, Double>();
        	    for (int i = 0 ;i < sameRID.size();i++ )
          	{
        		    String[] pair = sameRID.get(i).split("\t");
        		    int RID_2 = Integer.parseInt(pair[0]);
        		    double Sim = Double.parseDouble(pair[1]);
        		    map.put(RID_2, Sim);        		
        	    }
        	
            // using treemap to sort
        	    Map<Integer, Double> treeMap = new TreeMap<Integer, Double>(map);
        	    for (Map.Entry<Integer, Double> entry : treeMap.entrySet()) 
        	    {
        	        String id = entry.getKey().toString();
        	        String value = entry.getValue().toString();
        	        word.set("(" + key.toString() + "," + id + ")" + "\t" + value);        	        	
        	        context.write(word, NullWritable.get());
        	    }
        }
    }
    
    
	public static void main(String[] args) throws Exception {
		
        
		int NumOfReducer = Integer.parseInt(args[3]);		
        Configuration conf = new Configuration();
        conf.setDouble("similarity", Double.parseDouble(args[2]));
        Job job = Job.getInstance(conf, "set Similarit Join");
        job.setJarByClass(SetSimJoin.class);
        job.setMapperClass(RIDMapper.class);
        job.setReducerClass(RIDReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setNumReduceTasks(NumOfReducer);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        String output = args[1] + System.nanoTime();
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
        
        conf = new Configuration();
		job = Job.getInstance(conf, "Set Similarity Join");
		job.setJarByClass(SetSimJoin.class);
		job.setMapperClass(RemoveDuMapper.class);
		job.setReducerClass(RemoveDuReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(NumOfReducer);
		FileInputFormat.addInputPath(job, new Path(output));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
        
	}
}

