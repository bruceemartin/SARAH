package sarah.mapreduce.metrics;

/* This class extends the SarahMetricService with appropriate implementations in the Hadoop Map Reduce
 * Framework.  In particular,
 *  Save and Load are implemented in the file system
 *  Load adds all of the 
 *  Hadoop Configuration is used to pass SarahMetrics between the client and mappers and reducers.
 *  Hadoop counters are used to implement CountedSarahMetrics
 */

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import sarah.metrics.SarahMetricService;
import sarah.metrics.SarahMetricServiceBase;
import sarah.metrics.SarahMetrics;

public class MapReduceMetricService extends SarahMetricServiceBase implements SarahMetricService  {
	private TaskInputOutputContext<?, ?, ?, ?> context = null;
	private Job job = null;
	private Configuration conf = null;
	static MapReduceMetricService service;

	// Constructor used by clients
	public MapReduceMetricService(Job theJob, String pathName) throws IOException {
		super(FileSystem.get(theJob.getConfiguration()));
		job = theJob;
		conf = job.getConfiguration();
		sarahPathName = pathName;
		service = this;
		Path base = new Path(sarahPathName);
		Path statfile = new Path(base, MapReduceMetricService.sarahStatisticsName);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(statfile)) {
			conf.addResource(fs.open(statfile));
		}
		new MapReduceSarahMetrics(conf);
	}

	
	// Constructor used by mappers and reducers
	public MapReduceMetricService(TaskInputOutputContext<?, ?, ?, ?> ctx) throws IOException {
		super(FileSystem.get(ctx.getConfiguration()));
		context = ctx;
		conf = context.getConfiguration();
		service = this;
		new MapReduceSarahMetrics(conf);
	}


	
	Counter getCounter(String name) throws IOException {
		if (job!=null) return job.getCounters().findCounter(sarahTitle, name);
		else return context.getCounter(sarahTitle, name);
	}
	
	Counter getCounter(String functionName, String name) throws IOException {
		String counterName = "FUNCTION"+"."+functionName+"."+name;
		if (job!=null) return job.getCounters().findCounter(sarahTitle, counterName);
		else return context.getCounter(sarahTitle, counterName);
	}

	private String functionCounterToFunctionName(Counter counter) {
		String counterName = counter.getName();
		int endFunctionName = counterName.indexOf(".", 9);
		return counterName.substring(9, endFunctionName);
	}
	
	private String functionCounterToPattern(Counter counter) {
		String counterName = counter.getName();
		int endFunctionName = counterName.indexOf(".", 9);
		return counterName.substring(endFunctionName+1);
	}
	
	private boolean isFunctionCounter(Counter counter) {
		return counter.getName().startsWith("FUNCTION");
	}
	
	@Override
	public void save() throws IOException {
		// Add the computed values from the MapReduce counters to SarahMetrics
		Iterator<CounterGroup> counterGroups = job.getCounters().iterator();
		while (counterGroups.hasNext()) {
			CounterGroup cg = counterGroups.next();
			if (cg.getName().equals(sarahTitle)) {
				Iterator<Counter> counters = cg.iterator();
				while (counters.hasNext()) {
					Counter counter = counters.next();
					if (isFunctionCounter(counter)){
						SarahMetrics.get().setLong(functionCounterToFunctionName(counter), functionCounterToPattern(counter),counter.getValue());
					} else {
						SarahMetrics.get().setLong(counter.getName(), counter.getValue());	
					}
				}
			}
		}

		// Compute the actual sample size prior to saving the metrics
		long numberSampleRecords = SarahMetrics.get().numberSampleRecords.getValue();
		long numberInputRecords = SarahMetrics.get().numberInputRecords.getValue();
		double sample = ((double)numberSampleRecords/(double)numberInputRecords);
		SarahMetrics.get().sarahSampleFraction.setValue(sample);
		super.save();
	}


}
