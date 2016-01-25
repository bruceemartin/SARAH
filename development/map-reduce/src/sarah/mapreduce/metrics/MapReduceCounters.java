package sarah.mapreduce.metrics;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import sarah.metrics.SarahMetricServiceBase;

public class MapReduceCounters {
	static public TaskInputOutputContext<?, ?, ?, ?> context = null;
	static public Job job = null;
	
	private static HashMap<String,CountedMapReduceMetric> countedMetrics = new HashMap<String,CountedMapReduceMetric>();
	
	static void registerCountedMetric(CountedMapReduceMetric metric, String counterId) {
		if (countedMetrics.get(counterId)!=null) {
			throw new RuntimeException("Duplicate counterId "+counterId);
		}
		countedMetrics.put(counterId,metric);
	}

	// Mapper and Reducer cleanup methods call to add SarahMetrics to MR Counters
	public static void addSarahMetricsToCounters() throws IOException {
		for (CountedMapReduceMetric metric : countedMetrics.values()) {
			metric.setCounterValues();
		}
	}
	
	// Client calls to add counters to SarahMetrics
	public static void addCountersToSarahMetrics() throws IOException {
		Iterator<Counter> counters = job.getCounters().getGroup(SarahMetricServiceBase.sarahTitle).iterator();
		while (counters.hasNext()) {
			Counter counter = counters.next();
			countedMetrics.get(CountedLongMapReduceMetric.counterId(counter)).setValueFromCounter(counter);
		}
	}

}
