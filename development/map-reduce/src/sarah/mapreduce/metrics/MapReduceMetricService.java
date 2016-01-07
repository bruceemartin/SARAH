package sarah.mapreduce.util;

/* This class builds up statistics about the data set.  It uses Hadoop counters to collect the
 * statistics.  Some of the counters are parameterized by function name.
 * It uses Java properties stored in the Hadoop Configuration for parameters.
 * Some of the properties are parameterized by function name.
 * The difference between properties and counters is hidden from the client.
 * The object can be saved as a standard <configuration> XML file so that it may be used as
 * configuration for artifact generating program such as balance-reducers.
 * This object can also be reloaded from the saved XML file.
 * A simple human readable report can be written to a PrintStream.
 */

import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class SarahMapReduceMetrics implements SarahMetrics {
	private TaskInputOutputContext<?, ?, ?, ?> context = null;
	private String sarahCounterGroup = "Sarah Statistics";
	public static String sarahStatisticsName = "statistics.xml";
	private Job job = null;
	private boolean countersAdded=false;
	private String sarahPathName;
	private Configuration conf = null;
	private static double conversionFactor=1000;

	public SarahMapReduceMetrics(Job theJob, String pathName) {
		job = theJob;
		conf = job.getConfiguration();
		sarahPathName = pathName;
	}
	
	public SarahMapReduceMetrics(TaskInputOutputContext<?, ?, ?, ?> ctx) {
		context = ctx;
		conf = context.getConfiguration();
	}
	
	@Override
	public void increment(SarahMetric metric, String functionName, long amount) {
		context.getCounter(sarahCounterGroup,metric.name.replace("%func", functionName)).increment(amount);
	}
	
	@Override
	public void increment(SarahMetric metric, long amount) {
		context.getCounter(sarahCounterGroup,metric.name).increment(amount);
	}
	
	@Override
	public void save() throws IOException {
		addCountersToConfiguration();
		FileSystem fs = FileSystem.get(job.getConfiguration());
		Path p = new Path(sarahPathName, sarahStatisticsName);
		FSDataOutputStream out = fs.create(p);
		out.writeBytes(asXML());
		out.close();
	}

	@Override
	public void load() throws IOException {
		Path base = new Path(sarahPathName);
		Path statfile = new Path(base, SarahMapReduceMetrics.sarahStatisticsName);
		FileSystem fs = FileSystem.get(conf);
		conf.addResource(fs.open(statfile));
		String[] functions = conf.getStrings(SarahMetricNames.sarahFunctions.name);
		if (functions==null) {
			System.err.println("No functions specified in "+SarahMapReduceMetrics.sarahStatisticsName);
			System.exit(1);
		}
	}
	
	@Override
	public void print(PrintStream out) throws IOException {
		addCountersToConfiguration();
		out.println("\n#"+sarahCounterGroup);
		printMetrics(out,"Input Data Set",SarahMetricNames.inputDataSetMetrics);
		printMetrics(out,"Generated Sample",SarahMetricNames.sampleMetrics);
		printFunctionMetrics(out,"Functions",SarahMetricNames.functionMetrics,true);
		printFunctionMetrics(out,"Functions Applied to Sample",SarahMetricNames.sampleFunctionMetrics,false);	
	}

	@Override
	public long getLongValue(SarahMetric metric) throws IOException {
		// Return the value of the Hadoop counter.
		return job.getCounters().findCounter(sarahCounterGroup, metric.name).getValue();
	}

	@Override
	public long getLongValue(SarahMetric metric, String functionName) throws IOException {
		// Return the value of the Hadoop counter.
		return job.getCounters().findCounter(sarahCounterGroup, SarahMetricNames.stringForF(metric.name,functionName)).getValue();
	}

	@Override
	public String[] getFunctions() {
		return conf.getStrings(SarahMetricNames.sarahFunctions.name);
	}

	@Override
	public String getStringValue(SarahMetric metric) throws IOException {
		return conf.get(metric.name);
	}

	@Override
	public String getStringValue(String metricName) throws IOException {
		return conf.get(metricName);
	}
	
	private void addCountersToConfiguration() throws IOException {
		// This can only be done at client, after job has run
		if (countersAdded) return;
		// Add all metrics with destination counter to the configuration
		
		Iterator<SarahMetric> counterMetrics = SarahMetric.counters.iterator();
		while (counterMetrics.hasNext()) {
			SarahMetric metric = counterMetrics.next();
			Counter c = job.getCounters().findCounter(sarahCounterGroup,metric.name);
			if (c==null) continue;
			if (metric.type==SarahMetric.Type.Long) {
				conf.setLong(metric.name, c.getValue());
			} else if (metric.type==SarahMetric.Type.Double){
				conf.setDouble(metric.name, c.getValue()/conversionFactor);
			}
		}
		countersAdded=true;
	}

	private void printMetric(PrintStream out, SarahMetric metric) {
		out.println("\n# "+metric.description);
		out.println(metric.name+"="+conf.get(metric.name));
	}
	
	private void printMetrics(PrintStream out, String title, SarahMetric[] metrics) {
		out.println("\n\n\n# "+title);
		for (SarahMetric metric: metrics) {
			printMetric(out,metric);
		}
	}

	private void printFunctionMetrics(PrintStream out, String title, SarahMetric[] metrics, boolean addSarahFunctionsMetric) {
		out.println("\n\n# "+title);
		if (addSarahFunctionsMetric) {
			printMetric(out,SarahMetricNames.sarahFunctions);
		}
		String[] functions = conf.getStrings(SarahMetricNames.sarahFunctions.name);
		for (String function : functions) {
			printMetrics(out,"    "+function,functionMetricsForF(function, metrics));
		}
	}

	private void metricAsXML(StringBuilder result,SarahMetric metric) {
		result.append("    <property>\n");
		result.append("        <!-- "+metric.description+" -->");
		result.append("\n    <name>"+metric.name+"</name>\n    <value>");
		result.append(conf.get(metric.name)+"</value>\n    </property>\n");
	}
	
	private String asXML() throws IOException {
		StringBuilder result = new StringBuilder("<configuration>\n");
		metricsAsXML(result,"Input Data Set",SarahMetricNames.inputDataSetMetrics);
		metricsAsXML(result,"Generated Sample",SarahMetricNames.sampleMetrics);
		functionMetricsAsXML(result,"Functions",SarahMetricNames.functionMetrics,true);
		functionMetricsAsXML(result,"Functions Applied to Sample",SarahMetricNames.sampleFunctionMetrics,false);	
		result = result.append("</configuration>\n");
		return result.toString();
	}

	private void functionMetricsAsXML(StringBuilder result, String title, SarahMetric[] metrics, boolean addSarahFunctionsMetric) {
		result.append("\n    <!-- "+title+" -->\n");
		if (addSarahFunctionsMetric) {
			metricAsXML(result,SarahMetricNames.sarahFunctions);
		}
		String[] functions = conf.getStrings(SarahMetricNames.sarahFunctions.name);
		for (String function : functions) {
			metricsAsXML(result,function,functionMetricsForF(function, metrics));
		}		
	}

	private void metricsAsXML(StringBuilder result, String title, SarahMetric[] metrics) {
		result.append("\n    <!-- "+title+" -->\n");
		for (SarahMetric metric: metrics) {
			metricAsXML(result,metric);
		}
	}
	
	private SarahMetric[] functionMetricsForF(String function, SarahMetric[] metrics) {
		SarahMetric[] metricsForF = new SarahMetric[metrics.length];
		short i = 0;
		for (SarahMetric metric: metrics) {
			metricsForF[i++]=new SarahMetric(
										SarahMetricNames.stringForF(metric.name,function),
										SarahMetricNames.stringForF(metric.description,function),
										metric.type,
										metric.destination);
		}
		return metricsForF;
	}

	@Override
	public void setLongValue(SarahMetric metric, long amount) throws IOException {
		context.getCounter(sarahCounterGroup,metric.name).setValue(amount);
		
	}

	@Override
	public void setLongValue(SarahMetric metric, String function, long amount) {
		context.getCounter(sarahCounterGroup,SarahMetricNames.stringForF(metric.name,function)).setValue(amount);		
	}

	@Override
	public void setDoubleValue(SarahMetric metric, double amount) throws IOException {
		// Hadoop counters do not represent floating point values, need to convert
		context.getCounter(sarahCounterGroup,metric.name).setValue((long)(amount*conversionFactor));
		
	}

	@Override
	public void setDoubleValue(SarahMetric metric, String function, double amount) {
		// Hadoop counters do not represent floating point values, need to convert
		context.getCounter(sarahCounterGroup,SarahMetricNames.stringForF(metric.name,function)).setValue((long)(amount*conversionFactor));		
		
	}

	@Override
	public double getDoubleValue(SarahMetric metric) throws IOException {
		return context.getCounter(sarahCounterGroup,metric.name).getValue() / conversionFactor;
	}

	@Override
	public double getDoubleValue(SarahMetric metric, String function)
			throws IOException {
		return context.getCounter(sarahCounterGroup,SarahMetricNames.stringForF(metric.name,function)).getValue() / conversionFactor;		

	}

}
