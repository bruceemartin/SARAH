package sarah.mapreduce.statistics;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import sarah.mapreduce.metrics.FunctionKey;
import sarah.mapreduce.metrics.MapReduceMetricService;
import sarah.metrics.SarahMetrics;
import sarah.metrics.SarahMetricService;
import sarah.metrics.StatsForFunctionAndKey;


/*
 * 
 * The tool can be invoked to generate a random sample of intermediate data as follows:
 * 
 * 			% sarah statistics <mapper> <included-percentage> <data-set>
 * 
 * The sample-apply-function applies the map function in mapper class to
 * each record and the random sample is then taken on 
 * the output of the map function.  This supports generating a random sample of intermediate
 * data all in a single mapper.  This sample can then be used to understand the distribution
 * of the intermediate data.
 */

public class Statistics extends Configured implements Tool {
	private SarahMetricService sarahMetricService;
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 1) {
			System.out.printf("Usage: sarah [-libjars <jars>] [-conf <conf>] statistics <dataset>\n");
			return -1;
		}

		Job job = Job.getInstance(getConf());
		String sarahPathName = args[0]+".sarah";
		sarahMetricService = new MapReduceMetricService(job,sarahPathName);
		job.setJarByClass(Statistics.class);
		job.setJobName("sarah statistics");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(sarahPathName));

		job.setMapOutputKeyClass(FunctionKey.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setCombinerClass(LongSumReducer.class);
		
		buildMultipleOutputs(job);
		
		job.setNumReduceTasks(SarahMetrics.sarahFunctions.getValue().length);
		job.setPartitionerClass(MultiFunctionPartitioner.class);
		job.setMapperClass(MultiFunctionMapper.class);
		job.setCombinerClass(LongSumReducer.class);		
		job.setReducerClass(MultiFunctionReducer.class);

		boolean success = job.waitForCompletion(true);
		sarahMetricService.save();
		sarahMetricService.print(System.out);

		return success ? 0 : 1;
	}



	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void buildMultipleOutputs(Job job) throws ClassNotFoundException {
		// Keeps default output files from being created. 
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		// Need to handle missing properties gracefully.
		String sampleOutputFormat = job.getConfiguration().get(SarahMetrics.get().sarahSampleOutputFormat.name);
		String sampleOutputKeyClass= job.getConfiguration().get(SarahMetrics.get().sarahSampleOutputKeyClass.name);
		String sampleOutputValueClass = job.getConfiguration().get(SarahMetrics.get().sarahSampleOutputValueClass.name);
		
		for (String function : SarahMetrics.sarahFunctions.getValue()) {
			MultipleOutputs.addNamedOutput(job, function,
					(Class<? extends OutputFormat>) SequenceFileOutputFormat.class, 
					FunctionKey.class,
					StatsForFunctionAndKey.class);
		}
		// Add named output for generated random sample
		MultipleOutputs.addNamedOutput(job, "sarahrandom",
				(Class<? extends OutputFormat>) Class.forName(sampleOutputFormat), 
				Class.forName(sampleOutputKeyClass), 
				Class.forName(sampleOutputValueClass));

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Statistics(), args);
		System.exit(exitCode);
	}
}
