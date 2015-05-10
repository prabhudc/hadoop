/**
 * 
 */
package Test03;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.MRBench.Map;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * @author Prabhu
 *
 */
public class hadoopWC02 extends Configuration implements Tool {

	/**
	 * 
	 */

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new hadoopWC02(), args);
		System.exit(res);
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = new Job(conf,"hadoopWC02");
		job.setJarByClass(hadoopWC02.class);
		job.setMapperClass(map.class);
		job.setReducerClass(reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);
		TextInputFormat.addInputPath(job,new Path(arg0[0]));
		TextOutputFormat.setOutputPath(job,new Path(arg0[1]));
		job.setJobName("WordCount02");
		boolean res = job.waitForCompletion(true);
		if(res)
			return 0;
		else
			return -1;
		
	}

	private static class map extends Mapper<LongWritable,Text,Text,IntWritable>{
		IntWritable count = new IntWritable(1);
		String linerec = new String();
		//Text word = new Text();
		public void map(LongWritable key, Text values, Context context)throws IOException, InterruptedException {
		
		linerec = values.toString();				
		StringTokenizer tokens = new StringTokenizer(linerec);
		while(tokens.hasMoreTokens()){
			context.write(new Text(tokens.nextToken()), count);
			
		}
			
	}
	}
	private static class reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
	
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
			int sum = 0;		
			while(values.iterator().hasNext()){
				sum += values.iterator().next().get();				
				
			}
			context.write(key, new IntWritable(sum));
		}
		
	}
	
	
	
	
	
	
}
