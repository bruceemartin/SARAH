package sarah.mapreduce.balance;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import sarah.mapreduce.metrics.MapReduceCounters;
import sarah.metrics.SarahMetricService;
import sarah.metrics.SarahMetricServiceBase;

public class BalanceMetricService extends SarahMetricServiceBase implements SarahMetricService  {
	private Configuration conf = null;
	
	@Override
	public void save() throws IOException {
		super.save();
	}
	@Override 
	public void print(PrintStream out) throws IOException {
		super.printTool(out);		
		printMetrics(out,"Sarah Tool",BalanceSarahMetrics.get().toolMetrics);
		printMetrics(out,"Input Data Set",BalanceSarahMetrics.get().inputDataSetMetrics);
		printMetrics(out,"Generated Sample",BalanceSarahMetrics.get().sampleMetrics);
		printFunctionMetrics(out,"Functions",BalanceSarahMetrics.get().functionMetrics,true);
		printFunctionMetrics(out,"Functions Applied to Sample",BalanceSarahMetrics.get().sampleFunctionMetrics,false);	
	}
	@Override
	protected String asXML() throws IOException {
		StringBuilder result = new StringBuilder("<configuration>\n");
		metricsAsXML(result,"Sarah Tool",BalanceSarahMetrics.get().toolMetrics);
		metricsAsXML(result,"Input Data Set",BalanceSarahMetrics.get().inputDataSetMetrics);
		metricsAsXML(result,"Generated Sample",BalanceSarahMetrics.get().sampleMetrics);
		functionMetricsAsXML(result,"Functions",BalanceSarahMetrics.get().functionMetrics,true);
		functionMetricsAsXML(result,"Functions Applied to Sample",BalanceSarahMetrics.get().sampleFunctionMetrics,false);	
		result = result.append("</configuration>\n");
		return result.toString();
	}
	@Override
	public String getToolName() {
		return "SARAH Map-Reduce Balance Tool";
	}

	@Override
	public String getToolVersion() {
		return ".1";
	}
	
	// Constructor used by clients
	public BalanceMetricService(Job theJob,String pathName) throws IOException {
		super(FileSystem.get(theJob.getConfiguration()));
		MapReduceCounters.job = theJob;
		conf = theJob.getConfiguration();
		sarahPathName = pathName;
		//service = this;
		Path base = new Path(sarahPathName);
		Path statfile = new Path(base, BalanceMetricService.sarahStatisticsName);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(statfile)) {
			conf.addResource(fs.open(statfile));
		}
		new BalanceSarahMetrics(conf);
	}

	// Constructor used by mappers
	public BalanceMetricService(TaskInputOutputContext<?, ?, ?, ?> ctx) throws IOException {
		super(FileSystem.get(ctx.getConfiguration()));
		MapReduceCounters.context = ctx;
		conf = ctx.getConfiguration();
		//service = this;
	}

}
