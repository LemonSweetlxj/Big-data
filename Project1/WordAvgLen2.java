import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.htrace.commons.logging.Log;
import org.apache.htrace.commons.logging.LogFactory;


public class WordAvgLen2 {
	public static void main(String[] args) throws Exception {
		System.out.println("Testing");
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"Word Avg Length 2");
		job.setJarByClass(WordAvgLen2.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
				
	}
	
}


class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
	private static IntWritable wordLength = new IntWritable(1);
	private Text word = new Text();
	private IntWritable getLength(int length){
		return new IntWritable(length);
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
		while (itr.hasMoreTokens()){
			String w = itr.nextToken().toLowerCase();
			char c = w.charAt(0);
			if (c >= 'a' && c <= 'z'){
				word.set(String.valueOf(c));
				wordLength = this.getLength(w.length());
				context.write(word, wordLength);
			}
		
		
	}
}
}

class IntSumReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		DoubleWritable result = new DoubleWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			int totalNumberOfWords = 0;
			
			for (IntWritable val : values) {
				sum += val.get();
				totalNumberOfWords += 1;
			}
			result.set(sum/new Double(totalNumberOfWords).doubleValue());
			context.write(key, result);
			
			Log log = LogFactory.getLog(IntSumReducer.class);
			log.info("Mylog@Reducer: " + key.toString() + " " + result.toString());
			
			System.out.println(key.toString() + " " + result.toString());
		}
	}
	
