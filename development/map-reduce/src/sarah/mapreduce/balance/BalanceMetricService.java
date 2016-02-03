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
	private String statisticsFileName = "statistics.xml";
	private String balanceFileName = "artifacts/balance/balance.xml";
	private Path outputFilePath;
	
	// Constructor used by clients
	public BalanceMetricService(Job theJob,String pathName) throws IOException {
		super(FileSystem.get(theJob.getConfiguration()));
		sarahTitle = "Sarah Map-Reduce Balance";
		MapReduceCounters.job = theJob;
		conf = theJob.getConfiguration();
		Path base = new Path(pathName);
		Path statfile = new Path(base, statisticsFileName);
		outputFilePath = new Path(base,balanceFileName);
		FileSystem fs = FileSystem.get(conf);
		if (!fs.exists(statfile)) {
			throw new IOException("Cannot open "+statfile.getName());
		}
		conf.addResource(fs.open(statfile));
		new BalanceSarahMetrics(conf);
	}

	// Constructor used by mappers
	public BalanceMetricService(TaskInputOutputContext<?, ?, ?, ?> ctx) throws IOException {
		super(FileSystem.get(ctx.getConfiguration()));
		MapReduceCounters.context = ctx;
		conf = ctx.getConfiguration();
	}

	// Implementations SarahMetricService operations not implemented /overridden in SarahMetricServiceBase
	@Override
	public String getToolName() {
		return "SARAH Map-Reduce Balance Tool";
	}

	@Override
	public String getToolVersion() {
		return ".1";
	}
	
	@Override 
	public void print(PrintStream out) throws IOException {
		super.printTool(out);		
		printMetrics(out,"Sarah Tool",BalanceSarahMetrics.get().toolMetrics);
		printFunctionMetrics(out,"Number of reducer after these applying these functions",BalanceSarahMetrics.get().balanceMetrics,true);	
	}
	
	// Base class abstract method implementations
	@Override
	protected String asXML() throws IOException {
		StringBuilder result = new StringBuilder("<configuration>\n");
		metricsAsXML(result,"Sarah Tool",BalanceSarahMetrics.get().toolMetrics);
		functionMetricsAsXML(result,"Number of reducer after these applying these functions",BalanceSarahMetrics.get().balanceMetrics,true);	

		result = result.append("</configuration>\n");
		return result.toString();
	}

	@Override
	protected Path outputFilePath() {
		return outputFilePath;
	}

}
