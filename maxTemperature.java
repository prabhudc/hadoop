/**
 * 
 */
package Temperature;

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
public class maxTemperature extends Configuration implements Tool {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) 
			throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new maxTemperature(), args);
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
		Job job = new Job(conf);
		job.setJarByClass(maxTemperature.class);
		job.setJobName("MaxTemp");
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapperClass(MaxTempMapper.class);
		job.setNumReduceTasks(1);		
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setReducerClass(MaxTempReducer.class);
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		boolean out;
		out = job.waitForCompletion(true);
		if(out) {
			return 0;
		} else {
			return -1;
		}
		
		
	}
	
	public static class MaxTempMapper extends Mapper<LongWritable, Text, Text, IntWritable>	{
		
		String record = new String();

		
		public void map(LongWritable recno, Text rec, Context context) 
		throws IOException, InterruptedException {
		 record = rec.toString();
		 context.write(new Text(record.substring(15, 19)), new IntWritable(Integer.valueOf(record.substring(87, 92))));
		}
		
	}
	
	public static class MaxTempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private int maxTemp = 0;
	private int nextRec = 0;
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		throws IOException, InterruptedException {
			while(values.iterator().hasNext()){
				nextRec = values.iterator().next().get();
				if( nextRec > maxTemp && 
						nextRec != 9999) {
					maxTemp = nextRec;
				}
				
			}
			context.write(key, new IntWritable(maxTemp));
			//context.write(key, new IntWritable(1));
		}
	}

}
