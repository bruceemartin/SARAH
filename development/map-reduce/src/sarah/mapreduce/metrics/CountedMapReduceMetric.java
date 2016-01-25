package sarah.mapreduce.metrics;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Counter;

public interface CountedMapReduceMetric {
	void setCounterValues( ) throws IOException;
	void setValueFromCounter(Counter counter);
}
