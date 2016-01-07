package sarah.metrics;

import java.io.IOException;
import java.io.PrintStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public abstract class SarahMetricServiceBase implements SarahMetricService {
	protected static String sarahStatisticsName = "statistics.xml";
	protected String sarahPathName;
	protected String sarahTitle = "Sarah Statistics";
	protected FileSystem fs;


	public SarahMetricServiceBase(FileSystem fileSystem) {
		fs = fileSystem;
	}

	@Override
	public void save() throws IOException {
		Path p = new Path(sarahPathName, sarahStatisticsName);
		FSDataOutputStream out = fs.create(p);
		out.writeBytes(asXML());
		out.close();
	}

	@Override
	public void print(PrintStream out) throws IOException {
		out.println("\n#"+sarahTitle);
		printMetrics(out,"Input Data Set",SarahMetrics.get().inputDataSetMetrics);
		printMetrics(out,"Generated Sample",SarahMetrics.get().sampleMetrics);
		printFunctionMetrics(out,"Functions",SarahMetrics.get().functionMetrics,true);
		printFunctionMetrics(out,"Functions Applied to Sample",SarahMetrics.get().sampleFunctionMetrics,false);	
	}	
	



	
	private void metricAsXML(StringBuilder result,SarahMetric<?> metric) {
		result.append("    <property>\n");
		result.append("        <!-- "+metric.description+" -->");
		result.append("\n        <name>"+metric.name+"</name>\n        <value>");
		result.append(metric.getValue()+"</value>\n    </property>\n");
	}
	
	protected String asXML() throws IOException {
		StringBuilder result = new StringBuilder("<configuration>\n");
		metricsAsXML(result,"Input Data Set",SarahMetrics.get().inputDataSetMetrics);
		metricsAsXML(result,"Generated Sample",SarahMetrics.get().sampleMetrics);
		functionMetricsAsXML(result,"Functions",SarahMetrics.get().functionMetrics,true);
		functionMetricsAsXML(result,"Functions Applied to Sample",SarahMetrics.get().sampleFunctionMetrics,false);	
		result = result.append("</configuration>\n");
		return result.toString();
	}
	
	private SarahMetric<Object>[] functionMetricsForF(String function, SarahMetric<?>[] metrics) {
		SarahMetric<Object>[] metricsForF = new SarahMetric[metrics.length];
		short i = 0;
		for (SarahMetric<?> metric: metrics) {
			metricsForF[i]=new SarahMetric<Object>(
										SarahMetrics.get().stringForF(metric.name,function),
										SarahMetrics.get().stringForF(metric.description,function));
			metricsForF[i++].setValue(metric.getValue(function));
		}
		return metricsForF;
	}

	private void functionMetricsAsXML(StringBuilder result, String title, SarahMetric<?>[] functionMetrics, boolean addSarahFunctionsMetric) {
		result.append("\n    <!-- "+title+" -->\n");
		if (addSarahFunctionsMetric) {
			SarahMetric<String[]> metric = SarahMetrics.sarahFunctions;
			result.append("    <property>\n");
			result.append("        <!-- "+metric.description+" -->");
			result.append("\n        <name>"+metric.name+"</name>\n        <value>");
			result.append(toCommaSeparated(metric.getValue())+"</value>\n    </property>\n");
		}
		for (String function : SarahMetrics.sarahFunctions.getValue()) {
			metricsAsXML(result,function,functionMetricsForF(function, functionMetrics));
		}		
	}

	private void metricsAsXML(StringBuilder result, String title, SarahMetric<?>[] metrics) {
		result.append("\n    <!-- "+title+" -->\n");
		for (SarahMetric<?> metric: metrics) {
			metricAsXML(result,metric);
		}
	}
	private void printMetric(PrintStream out, SarahMetric<?> metric) {
		out.println("\n# "+metric.description);
		out.println(metric.name+"="+metric.getValue());
	}
	
	private void printMetrics(PrintStream out, String title, SarahMetric<?>[] inputDataSetMetrics) {
		out.println("\n\n\n# "+title);
		for (SarahMetric<?> metric: inputDataSetMetrics) {
			printMetric(out,metric);
		}
	}

	private void printFunctionMetrics(PrintStream out, String title, SarahMetric<?>[] functionMetrics, boolean addSarahFunctionsMetric) {
		out.println("\n\n# "+title);
		if (addSarahFunctionsMetric) {
			SarahMetric<String[]> metric = SarahMetrics.sarahFunctions;
			out.println("\n# "+metric.description);
			out.println(metric.name+"="+toCommaSeparated(metric.getValue()));
		}
		for (String function : SarahMetrics.sarahFunctions.getValue()) {
			printMetrics(out,"    "+function,functionMetricsForF(function, functionMetrics));
		}
	}

	private String toCommaSeparated(String[] functionNames) {
		String result="";
		int numberFunctionNames = functionNames.length;
		for (int i=0; i<numberFunctionNames; i++) {
			result=result+functionNames[i];
			if (i!=numberFunctionNames-1) {
				result=result+",";
			}
		}
		return result;
	}
}
