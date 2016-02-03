package sarah.mapreduce.metrics;

/* This class extends the SarahMetricService with appropriate implementations in the Hadoop Map Reduce
 * Framework.  In particular,
 *  Save and Load are implemented in the file system
 *  Load adds all of the 
 *  Hadoop Configuration is used to pass SarahMetrics between the client and mappers and reducers.
 *  Hadoop counters are used to implement CountedSarahMetrics
 */

import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import sarah.metrics.SarahMetricService;
import sarah.metrics.SarahMetricServiceBase;
import sarah.metrics.SarahMetrics;

public class MapReduceMetricService extends SarahMetricServiceBase implements SarahMetricService  {
	private Configuration conf = null;
	private String sarahStatisticsName = "statistics.xml";
	private Path statfile;
	
	// Constructor used by clients
	public MapReduceMetricService(Job theJob, String pathName) throws IOException {
		super(FileSystem.get(theJob.getConfiguration()));
		MapReduceCounters.job = theJob;
		conf = theJob.getConfiguration();
		Path base = new Path(pathName);
		statfile = new Path(base, sarahStatisticsName);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(statfile)) {
			conf.addResource(fs.open(statfile));
		}
		new MapReduceSarahMetrics(conf);
	}
	
	// Constructor used by mappers and reducers
	public MapReduceMetricService(TaskInputOutputContext<?, ?, ?, ?> ctx) throws IOException {
		super(FileSystem.get(ctx.getConfiguration()));
		MapReduceCounters.context = ctx;
		conf = ctx.getConfiguration();
		if (MapReduceSarahMetrics.get()==null) {
			new MapReduceSarahMetrics(conf);
		}
	}
	
	@Override
	public String getToolName() {
		return "SARAH Map-Reduce Statistics Generation Tool";
	}

	@Override
	public String getToolVersion() {
		return ".1";
	}

	@Override
	public void print(PrintStream out) throws IOException {
		super.printTool(out);
		printMetrics(out,"Sarah Tool",SarahMetrics.get().toolMetrics);
		printMetrics(out,"Input Data Set",SarahMetrics.get().inputDataSetMetrics);
		printMetrics(out,"Generated Sample",SarahMetrics.get().sampleMetrics);
		printFunctionMetrics(out,"Functions",SarahMetrics.get().functionMetrics,true);
		printFunctionMetrics(out,"Functions Applied to Sample",SarahMetrics.get().sampleFunctionMetrics,false);	
	}
	
	@Override
	public void save() throws IOException {
		// Add the computed values from the MapReduce counters to SarahMetrics
		MapReduceCounters.addCountersToSarahMetrics();
		// Compute the actual sample size prior to saving the metrics
		long numberSampleRecords = SarahMetrics.get().numberSampleRecords.getValue();
		long numberInputRecords = SarahMetrics.get().numberInputRecords.getValue();
		double sample = ((double)numberSampleRecords/(double)numberInputRecords);
		SarahMetrics.get().sarahSampleFraction.setValue(sample);
		super.save();
	}
	
	protected String asXML() throws IOException {
		StringBuilder result = new StringBuilder("<configuration>\n");
		metricsAsXML(result,"Sarah Tool",SarahMetrics.get().toolMetrics);
		metricsAsXML(result,"Input Data Set",SarahMetrics.get().inputDataSetMetrics);
		metricsAsXML(result,"Generated Sample",SarahMetrics.get().sampleMetrics);
		functionMetricsAsXML(result,"Functions",SarahMetrics.get().functionMetrics,true);
		functionMetricsAsXML(result,"Functions Applied to Sample",SarahMetrics.get().sampleFunctionMetrics,false);	
		result = result.append("</configuration>\n");
		return result.toString();
	}
	
	@Override
	protected Path outputFilePath() {
		return statfile;
	}










}
