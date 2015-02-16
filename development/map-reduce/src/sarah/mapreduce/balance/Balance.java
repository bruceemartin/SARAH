package sarah.mapreduce.balance;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import sarah.mapreduce.util.SarahMapReduceMetrics;
import sarah.mapreduce.util.SarahMetricNames;
import sarah.mapreduce.util.SarahMetrics;



/*
 * 
 * The tool generates balanced reducer artifacts
 * 
 * 			% sarah [-conf <conf>] balanced-reducers <dataset>
 * 
 */

public class Balance extends Configured implements Tool {

	private String baseDir="";
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 1) {
			System.out
					.printf("Usage: sarah [-conf <conf>] balanced-reducers <dataset>\n");
			return -1;
		}
		baseDir = args[0];
		Job job = Job.getInstance(getConf());
		SarahMetrics sarahMetrics = new SarahMapReduceMetrics(job,baseDir+".sarah");
		sarahMetrics.load();
		job.setJarByClass(Balance.class);
		job.setJobName("sarah balanced-reducers");
		buildInputsAndOutputs(job);
		job.setMapperClass(BalanceMapper.class);
		job.setNumReduceTasks(0);
		boolean success = job.waitForCompletion(true);
		System.out.print("Balanced reducers using the following properties: ");
		sarahMetrics.print(System.out);
		return success ? 0 : 1;
	}


	private void buildInputsAndOutputs(Job job) throws ClassNotFoundException, IllegalArgumentException, IOException {
		// Need to handle missing properties gracefully.
		// Keeps default output files from being created. 
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		String[] functions = job.getConfiguration().getStrings(SarahMetricNames.sarahFunctions.name);
		
		FileOutputFormat.setOutputPath(job, new Path(baseDir+".sarah/artifacts/balanced-reducers"));
		job.setInputFormatClass(SequenceFileInputFormat.class); 
		for (String function : functions) {
			FileInputFormat.addInputPath(job, new Path(baseDir+".sarah/functions/"+function));
			String functionOutputKeyClass = job.getConfiguration().get(SarahMetricNames.stringForF(SarahMetricNames.sarahOutputKeyForF.name, function));
			// sequence file for TotalOrderPartitioner for the function
			MultipleOutputs.addNamedOutput(job, function,
					SequenceFileOutputFormat.class, 
					Class.forName(functionOutputKeyClass), NullWritable.class);
			// text file for human readable form of the interval file
			MultipleOutputs.addNamedOutput(job, function+"txt", TextOutputFormat.class, Text.class, NullWritable.class);
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Balance(), args);
		System.exit(exitCode);
	}
}
