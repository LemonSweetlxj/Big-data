import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordAvgLen1 {
	
	public static class IntPair extends ArrayWritable{
		public IntPair(){
			super(IntWritable.class);
		}
		
		public IntPair(IntWritable[] writableValues){
			super(IntWritable.class, writableValues);			
		}
		
		public IntWritable[] get(){
			Writable[] writables = super.get();
			IntWritable length = (IntWritable) writables[0];
			IntWritable count = (IntWritable) writables[1];
			return new IntWritable[]{length,count};
		}		
	}
	
	
	public static class Useful{
		public static IntWritable[] getIntWritable(int length, int count){
			return new IntWritable[]{new IntWritable(length),new IntWritable(count)};
		}
	
		public static IntWritable[] getIntWritable(int length){
			return getIntWritable(length,1);
		}
	}
	
	
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntPair>{
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
			
			while (itr.hasMoreTokens()){
				String w = itr.nextToken().toLowerCase();
				char c = w.charAt(0);
				IntPair intPair = null;
				if (c >= 'a' && c <= 'z'){
					word.set(String.valueOf(c));
					intPair = new IntPair(Useful.getIntWritable(w.length()));
					context.write(word, intPair);
				}			
			}		
		}
	}
	
	
	public static class MyCombiner extends Reducer<Text, IntPair,Text, IntPair>{
		public void Combiner(Text key, Iterable<IntPair> values, Context context)
		                            throws IOException, InterruptedException{
			int sum = 0;
			int totalNumberOfWords = 0;
			
			for(IntPair eachValue : values){
				IntWritable[] totalData = eachValue.get();
				sum += totalData[0].get();
				totalNumberOfWords += totalData[1].get();
			}
			context.write(key, new IntPair(Useful.getIntWritable(sum, totalNumberOfWords)));			
		}		
	}
	
	
	public static class IntSumReducer extends Reducer<Text, IntPair, Text, DoubleWritable>{
		DoubleWritable result = new DoubleWritable();
		
		public void reduce(Text key, Iterable<IntPair> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			int totalNumberOfWords = 0;
			
			for( IntPair eachValue : values){
				IntWritable[] totalData = eachValue.get();
				sum += totalData[0].get();
				totalNumberOfWords += totalData[1].get();
				}
			result.set(sum/new Double(totalNumberOfWords).doubleValue());
			context.write(key, result);
			
		}
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println("Testing");
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"Word Avg Length 1");
		job.setJarByClass(WordAvgLen1.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(MyCombiner.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntPair.class);
	     
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
				
	}
	
	
	

}
