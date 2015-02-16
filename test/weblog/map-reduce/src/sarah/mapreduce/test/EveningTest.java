package sarah.mapreduce.test;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class EveningTest extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3 ) {
			System.out.printf("Usage:  <reducers> <input> <output>\n");
			return -1;
		}

		Job job = Job.getInstance(getConf());
		job.setJarByClass(EveningTest.class);
		job.setJobName("sarah evening test");

		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setMapperClass(EveningFilterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(RecordCounterReducer.class);
		job.setNumReduceTasks(Integer.parseInt(args[0]));
		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new EveningTest(), args);
		System.exit(exitCode);
	}
}
