package sarah.metrics;

import java.io.IOException;
import java.io.PrintStream;

/* Sarah tools use this interface for collecting metrics defined in SarahMetrics.
 * While different implementations are possible, currently MapReduceMetricService and 
 * BalanceMetricService implements this interface.
 */
public interface SarahMetricService {
	void save() throws IOException;
	void print(PrintStream out) throws IOException;
	String getToolName();
	String getToolVersion();
}
