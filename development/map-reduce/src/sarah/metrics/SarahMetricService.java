package sarah.mapreduce.util;

import java.io.IOException;
import java.io.PrintStream;

/* Sarah tools use this interface for collecting metrics defined in SarahMetricNames.
 * While different implementations are possible, currently only SarahMapReduceMetrics
 * implements this interface.
 * Note:  some of these operations are only available from the client, some from the 
 * cluster and the client.  This should probably be separated into different interfaces
 */
public interface SarahMetrics {
	void save() throws IOException;
	void load() throws IOException;
	void print(PrintStream out) throws IOException;
	String[] getFunctions();
	long getLongValue(SarahMetric metric) throws IOException;
	void setLongValue(SarahMetric metric, long amount) throws IOException;
	long getLongValue(SarahMetric metric, String function) throws IOException;
	void setLongValue(SarahMetric metric, String function, long amount);
	double getDoubleValue(SarahMetric metric) throws IOException;
	void setDoubleValue(SarahMetric metric, double amount) throws IOException;
	double getDoubleValue(SarahMetric metric, String function) throws IOException;
	void setDoubleValue(SarahMetric metric, String function, double amount);
	String getStringValue(SarahMetric metric) throws IOException;
	String getStringValue(String metricName) throws IOException;
	void increment(SarahMetric metric, long amount);
	void increment(SarahMetric metric, String function, long amount);
}
