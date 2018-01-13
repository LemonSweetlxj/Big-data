package comp9313.ass2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




/**
 * @author Xinjie LU  z5101488  
 * Finished on 17/09
 * This is a project to find the shortest path(distance and path) using dijkstra.
 * The input file is a graph contained edge, source node, destination node, distance.
 * The goal is to find the distance and path from a given node to other nodes.
 * There are three map reduce steps used:
 * 1. convert the input form  ---JOB1
 * 2. find distance and path ----JOB2
 * 3. write the result into a output file --JOB3
 * 
 */
public class SingleSourceSP {


    public static String OUT = "output";
    public static String IN = "input";
    
    public static int queryNodeID;   
    //set the enum COUNTER
    static enum eInf {
      COUNTER
    }
    
    /*
     * JOB1:
     * Mapper:
     * 	The input format is edge id,fromNode,toNode and distance
     *  The edge id we do not use, so after mapper, the data structure:
     *  key:  fromNodeId   value: toNodeId:distance
     *  
     * Reducer:
     *   Put the values together with the same key.
     *   If the key is exactly the queryId, set its distance to 0.0,
     *      else set it distance to -1.0(infinity)
     *   At the same time, we use a flag to represent whether this node has been visited,
     *   "U" represents Undiscovered and "D" represents Discovered.
     *   Use "|" to split the source node information and adjacent information
     *   After reducer, the data structure:
     *   key: sourceNode  value: initialDistance  status|adjNode1:distance1, adjNode2:distance2.....
     */
    public static class AdjacencyListMapper extends Mapper<Object, Text, IntWritable, Text>
     {
      //create text parameters id,fromNode,toNode and distance
           
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String[] strings = value.toString().split(" ");
        	System.out.println(Arrays.toString(strings));
			String fromNodeId = strings[1];
			Integer sourceNodeInt = Integer.parseInt(fromNodeId);
			String toNodeIdAndDist = strings[2] + ":" + strings[3];
			
			context.write(new IntWritable(sourceNodeInt), new Text(toNodeIdAndDist));
            }
        }
    
    public static class AdjacencyListReducer extends Reducer<IntWritable, Text, IntWritable, Text> 
    {   
         private Text t = new Text();
         private int queryNodeID = 0;
       
       public void setup(Context context)
       {
           Configuration config = context.getConfiguration();
           queryNodeID = config.getInt("queryNodeID", 0);
       }
       public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
       {
         StringBuffer sb = new StringBuffer();
           for (Text val : values) 
           {
               if(sb.length() == 0)
               {
                   sb = sb.append(val.toString()); //if the sb is empty, sb add the string (val.toString())
                }
               else
               {
                   sb = sb.append(",".concat(val.toString())); //else , add new string with the symbol ","
                }
             }
           String str="-1.0"+"\t" + "U|"; //the string for other from node
           if (key.get() == queryNodeID) str="0.0" + "\t" + "U|"; //the string for aim node(querynode)
         t.set(str + sb.toString()); //combine the str with sb string
         context.write(key, t);
        }
    }

    /*
     * JOB2:
     * 	Mapper:
     *  1.split the input form into [sourceDetail] and {adjacentDetail} and delete the space in sourceDetail
     *  2.Deal the soureDetail: [sourceNode, sourceWeight, sourceStatus,(currenPath)]
     *     if there is no currentPath, the current Path == sourceNode
     *     if there is currentPath, the current Path += courceNode
     *     Then set this node Discovered
     *     write the information into output
     *  3.Deal the adjacentDetail:{adjNode1:distance1, adjNode2:distance2,...}
     *     adj distance += sourceWeight
     *     and set this node Undiscovered
     *     write the information into output
     *     if the sourceNode has no adjNode, it means that this is a node can not reach.
     *     the distance -1.0 also proves.
     *  4.If the source Node has been discovered, then the information of this node does not change
     *  
     *  The output of mapper:
     *  1.for the original input
     *  key: sourceNode  value:sourceWeight ourceStatus (currenPath)|adjNode1:distance1, adjNode2:distance2....
     *  2.for the adjNode(update):
     *  key: adjNode  value:distance Status (currentPath)|
     *  
     */
    public static class SSSPMapper extends Mapper<Object, Text, LongWritable, Text>
     {

        @Override
       public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {

            if (value.toString().trim().equals(""))
            {
                return;
            }
                    
            String[] strings = value.toString().split("\\|");
            String[] sourceDetail = strings[0].split("\t"); 
            String[] tokens = {};
            if (strings.length == 2)
            {
                tokens = strings[1].split(",");
            }

            // delete the space in sourceNode information
            List<String> tmp = new ArrayList<String>();  
            for(String str:sourceDetail)
            {  
                if(str!=null && str.length()!=0)
                {  
                    tmp.add(str);  
                }  
            }  
            sourceDetail = tmp.toArray(new String[0]);  


            String sourceNode = sourceDetail[0];
            Integer sourceNodeInt = Integer.parseInt(sourceNode);

            String sourceWeight = sourceDetail[1];
            Double sourceWeighttDouble = Double.parseDouble(sourceWeight);
            String surceStatus = sourceDetail[2];
            String currentPath = "";

             if (sourceDetail.length >= 3
                && !sourceWeight.equalsIgnoreCase("-1.0")
                && !surceStatus.equalsIgnoreCase("D"))
             {
               
            	//set path
                if (sourceDetail.length == 4)
                {
                    currentPath = sourceDetail[3];
                }
                
                currentPath += (currentPath.length() == 0 ? "" : "->") + sourceDetail[0];
                
                String word = "";
                //has been discovered
                if (strings.length != 1) 
                { 
                    word = sourceWeight + "\t" + "D" + "\t" + currentPath + "|" + strings[1];
                    context.write(new LongWritable(sourceNodeInt), new Text(word));
                }
                else
                {
                    word = sourceWeight + "\t" + "D" + "\t" + currentPath + "|";
                    context.write(new LongWritable(sourceNodeInt), new Text(word));
                }

                
             
                //has adjNode
                if (tokens.length> 0)
                {
                    String[][] adjacentNodeDetails = new String[tokens.length][2];
                    for (int index = 0; index < tokens.length; index++)
                    { 
                        adjacentNodeDetails[index] = tokens[index].split(":");
                    }

                    for (int index = 0; index < tokens.length; index++)
                        {
                            Double number = sourceWeighttDouble + Double.parseDouble(adjacentNodeDetails[index][1]);
                            Integer adjacentNodeInt = Integer.parseInt(adjacentNodeDetails[index][0]);
                            String adjacentNode = Double.toString(number) + "\t" + "U" + "\t" + currentPath;
                            context.write(new LongWritable(adjacentNodeInt), new Text(adjacentNode));
                     
                        }
                }
            }
             //the information does not change
            else
            {
                int n = sourceNode.length(); 
            	String noChange = value.toString().substring(n);
                context.write(new LongWritable(sourceNodeInt),new Text(noChange));
                 
            }

        }

     }

    /*
     * Reducer:
     * For every value:
     *   "|" to distinct whether it is original information or update information.
     *   if it is update information, get the distance and path.
     *   compared the distance with the sourceNode current value.
     *   if the distance is smaller, it means we need to update the distance of the source Node.
     *   so change the sourceNode weight and new path.
     *   if it does not change, its distance and path remains still and set this Node discovered.
     *   write the information to the output.
     * 
     * The output of reducer:
     * key: sourceNode Value: newDistance newStatus newPath|adjNode1:distance1, adjNode2:distance2....
     * 
     */
    public static class SSSPReducer extends Reducer<LongWritable, Text, LongWritable, Text> 
    {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            
            String origAdjacencyList = "";
            ArrayList<Double> updatedDistances = new ArrayList<Double>();
            ArrayList<String> updatedPath = new ArrayList<String>();

            for (Text updatedDistOrAdjList : values) 
            {
                String updatedDistOrAdjListStr = updatedDistOrAdjList.toString();                

                if (updatedDistOrAdjListStr.indexOf('|') == -1) 
                {
                    // Contains no "|" means it is update information
                    String[] updateList = updatedDistOrAdjList.toString().split("\t");
                    
                    // delete the space
                    List<String> tmp = new ArrayList<String>();  
                    for(String str:updateList)
                    {  
                        if(str!=null && str.length()!=0)
                        {  
                            tmp.add(str);  
                        }  
                    }  
                    updateList = tmp.toArray(new String[0]);
                    
                    //new distance
                    String newDistance = updateList[0];                 
                    Double distance = Double.parseDouble(newDistance);
                    
                    //new path
                    String path = updateList[2];                    
                    updatedDistances.add(distance);
                    updatedPath.add(path);
                
                } 
                else 
                {
                    // Contains "|" means it is original information
                    origAdjacencyList = updatedDistOrAdjListStr;
                }
            }

           
            String[] strings = origAdjacencyList.split("\\|");
            String[] former = strings[0].split("\\t");
            
            // delete the space
            List<String> tmp = new ArrayList<String>();  
            for(String str:former)
            {  
                if(str!=null && str.length()!=0)
                {  
                    tmp.add(str);  
                }  
            }  
            former = tmp.toArray(new String[0]);
            
            String origSrcDistStr = "";
            Double currentDistance = -1.0;
            String currentPath = "";
            
            //no adjNode
            if (!origAdjacencyList.equals("")) 
            {
                origSrcDistStr = former[0];
                currentDistance = Double.parseDouble(origSrcDistStr);
                if(former.length == 3){
                    currentPath = former[2];
                }
            }

            String minPath = "";
            Double minDistance = Double.POSITIVE_INFINITY;
            for (int i = 0; i < updatedDistances.size(); i++) {
                if (updatedDistances.get(i) < minDistance) {
                    minDistance = updatedDistances.get(i);
                    minPath = updatedPath.get(i);
                }
            }

            //need to change the distance and path
            if (minDistance != Double.POSITIVE_INFINITY && (currentDistance == -1.0 || minDistance < currentDistance)) {
                String minDistanceStr = String.valueOf(minDistance);
                // Just in case that origAdjacencyList is "" or "dist|"
                if (strings.length >= 2) {
                    origAdjacencyList = minDistanceStr + "\t" + "U" +"\t" + minPath + "|" + strings[1];
                } else {
                    origAdjacencyList = minDistanceStr + "\t" + "U" +"\t" + minPath+ "|";
                }

                context.getCounter(eInf.COUNTER).increment(1);
            }

            //distance no change
            else 
            {   
                //origAdjacencyList = orignal;

                if (strings.length >= 2) {
                    origAdjacencyList = origSrcDistStr + "\t" + "D" + "\t" + currentPath + "|" + strings[1];
                } else {
                    origAdjacencyList = origSrcDistStr + "\t" + "D" + "\t" + currentPath+ "|";
                }
            }
            context.write(key, new Text(origAdjacencyList));
        }
    }


    
    /*
     * JOB3
     * Mapper:
     *   the output of mapper:
     *   key:toNode value: distance+path
     *   if the distance is -1.0
     *   it means the node cannot be reached
     *   just return
     *   
     * Reducer:
     *  the output of reducer:
     *  key: null  value: toNode + distance + path
     */
    public static class FinalWriteMapper extends Mapper<Object, Text, IntWritable, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Get rid of empty line(s)
            if (value.toString().trim().equals("")){
                return;
            }
            
            
            String line = value.toString().split("\\|")[0];
            String[] toNodeIdAndDistArray = line.split("\t");
           
            List<String> tmp = new ArrayList<String>();  
            for(String str:toNodeIdAndDistArray)
            {  
                if(str!=null && str.length()!=0)
                {  
                    tmp.add(str);  
                }  
            }  
            toNodeIdAndDistArray = tmp.toArray(new String[0]);
            //System.out.println("final:" + Arrays.toString(toNodeIdAndDistArray));
            
            Integer toNodeId = Integer.parseInt(toNodeIdAndDistArray[0]);
            String dist = toNodeIdAndDistArray[1];
            
            // In case the node is unreachable from the source node, then just ignore it
            if( Double.parseDouble(dist) == -1.0 ){
                return;
            }
            String path = toNodeIdAndDistArray[3];
            
            // Use the "toNodeId" as key to guarantee the targetNodes are sorted in numerical order
            String write = dist + "\t" + path;
            context.write(new IntWritable(toNodeId), new Text(write));
            //System.out.println("finalMapper:" + toNodeId.toString() + write.toString() );
        }
    }
    public static class FinalWriteReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Get the queryNodeId (done by Configuration get/set) from outside 
            /*
             * 
             * ..................................
             */
            //Configuration conf = context.getConfiguration();
            //String queryNodeId = conf.get("queryNodeId");
            
            String toNodeId = String.valueOf(key.get());
            
            for (Text value : values) {
                String dist = value.toString();
                context.write( NullWritable.get(), new Text(toNodeId+"\t"+dist));
            }
        }
    }

    public static void main(String[] args) throws Exception {        

        IN = args[0];

        OUT = args[1];

        int iteration = 0;
        
        int queryNodeID = Integer.parseInt(args[2]);
        
        String input = IN;

        String output = OUT + iteration;
        
        Configuration conf = new Configuration();
        conf.setInt("queryNodeID", queryNodeID);
        Job job1 = Job.getInstance(conf, "Adjacency List");
        job1.setJarByClass(SingleSourceSP.class);
        job1.setMapperClass(AdjacencyListMapper.class);
        job1.setReducerClass(AdjacencyListReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        job1.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(output));
        
        job1.waitForCompletion(true);

        // YOUR JOB: Convert the input file to the desired format for iteration, i.e., 
        //           create the adjacency list and initialize the distances
        // ... ...

        boolean isdone = false;

        while (isdone == false) {

            // YOUR JOB: Configure and run the MapReduce job
            // ... ...                   
            Job job2 = Job.getInstance(conf, "SS Mapper");
            job2.setJarByClass(SingleSourceSP.class);
            job2.setMapperClass(SSSPMapper.class);
            job2.setReducerClass(SSSPReducer.class);
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Text.class);
            job2.setNumReduceTasks(1);
            
            input = output;    

            iteration ++;       

            output = OUT + iteration;
            
            FileInputFormat.addInputPath(job2, new Path(input));
            FileOutputFormat.setOutputPath(job2, new Path(output));
            

            
            job2.waitForCompletion(true);
            
            Configuration confs = new Configuration();
            FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"),confs);
            if (fs.exists(new Path(output)))
            {
            	fs.delete(new Path(input),true);
            }
                           
            //You can consider to delete the output folder in the previous iteration to save disk space.

            // YOUR JOB: Check the termination criterion by utilizing the counter
            // ... ...

            if(job2.getCounters().findCounter(eInf.COUNTER).getValue() == 0){
                isdone = true;
            }
        }
        while (isdone == true) {
            Job job3 = Job.getInstance(conf, "Result");
            job3.setJarByClass(SingleSourceSP.class);
            job3.setMapperClass(FinalWriteMapper.class);
            job3.setReducerClass(FinalWriteReducer.class);
            job3.setOutputKeyClass(IntWritable.class);
            job3.setOutputValueClass(Text.class);
            job3.setNumReduceTasks(1);
            
            input = output;

            iteration ++;       

            output = OUT + iteration;
            
            FileInputFormat.addInputPath(job3, new Path(input));
            FileOutputFormat.setOutputPath(job3, new Path(OUT));
            
            
           job3.waitForCompletion(true);
            
            Configuration confs = new Configuration();
            FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"),confs);
            if (fs.exists(new Path(OUT)))
            {
            	fs.delete(new Path(input),true);
            }
            
            System.exit(job3.waitForCompletion(true) ? 0 : 1);
                      
        }

        // YOUR JOB: Extract the final result using another MapReduce job with only 1 reducer, and store the results in HDFS
        // ... ...
    }

}
