import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class SalesInfo {

	public static class SalesMap extends
		Mapper<LongWritable, Text, Text, IntWritable>{

			//setup, map, run, cleanup

			public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
					String line = value.toString();
					String[] elements = line.split(","); 		// element[0] 1, element[1] amazon
					Text tx = new Text(elements[1]); 			//amazon
					int i = Integer.parseInt(elements[3]); 		//string to int
					IntWritable it = new IntWritable(i);	    //element[3] 2000
					context.write(tx, it);

			}
	}

	public static class SalesReduce extends 
		Reducer<Text, IntWritable, Text, IntWritable>{

			//setup, reduce, run, cleanup
			//input amazon [2000, 1000, 5500]
			public void reduce(Text key, Iterable<IntWritable>values, 
				Context context) throws IOException, InterruptedException {
					int sum = 0; 
					for(IntWritable val: values){
						sum += val.get();
					}
					context.write(key, new IntWritable(sum));
			}
	}

	public static void main(String[] args) throws Exception {

		if(args.length != 2) {
			System.err.println("Argumentos insuficientes");
			System.exit(-1);
		}
		Configuration conf = new Configuration();

		//conf.set("fs.default.name", "hdfs://localhost:50001");
		conf.set("mapred.job.tracker", "hdfs://localhost:50001");

		//conf.set("DrugName", args[3]);
		Job job = new Job(conf, "Drug Amount Spent");

		job.setJarByClass(SalesInfo.class); //Class contains mapper and reducer class


		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);


		job.setMapperClass(SalesMap.class);
		job.setReducerClass(SalesReduce.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(TextInputFormat.class); //Default -- inputkey
														//Type -- longwritable
														//valuetyep is text
		job.setOutputFormatClass(TextOutputFormat.class);



		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		
	}
	
}
