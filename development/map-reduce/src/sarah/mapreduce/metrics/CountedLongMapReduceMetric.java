package sarah.mapreduce.metrics;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;

import sarah.metrics.CountedLongSarahMetric;
import sarah.metrics.SarahMetricServiceBase;
import sarah.metrics.SarahMetrics;

public class CountedLongMapReduceMetric extends CountedLongSarahMetric implements CountedMapReduceMetric {

	private String counterId="";
	
	public CountedLongMapReduceMetric(CountedLongSarahMetric metric, String counterId, Configuration conf) {
		super(metric.name,metric.description);
		if (!parameterizedByFunction()) {
			// If the metric has a value in the configuration, use it.
			setValue(conf.getLong(name, 0));
		} else {
			for (String function: SarahMetrics.sarahFunctions.getValue()) {
				String propertyName = SarahMetrics.get().stringForF(name, function);
				// If the metric has a value in the configuration, use it.
				setValue(function,conf.getLong(propertyName,0));
			}
		}
		this.counterId = counterId;
		MapReduceCounters.registerCountedMetric(this,counterId);
	}

	public void setCounterValues( ) throws IOException {
		if (parameterizedByFunction()) {
			for (String functionName: functionNames()) {
				Counter counter = MapReduceCounters.context.getCounter(SarahMetricServiceBase.sarahTitle, counterId+"/"+functionName);
				counter.increment(getValue(functionName));
			}
		} else {
			Counter counter = MapReduceCounters.context.getCounter(SarahMetricServiceBase.sarahTitle, counterId);
			counter.increment(getValue());
		}
	}

	@Override
	public void setValueFromCounter(Counter counter) {
		if (parameterizedByFunction()) {
			setValue(functionName(counter),counter.getValue());
		} else {
			setValue(counter.getValue());
		}
	}

	static Object counterId(Counter counter) {
		String counterName = counter.getName();
		int separator = counterName.indexOf('/');
		if (separator == -1) return counterName;
		else return counterName.substring(0,separator);
	}
	
	static String functionName(Counter counter) {
		String counterName = counter.getName();
		int separator = counterName.indexOf('/');
		if (separator == -1) return "";
		else return counterName.substring(separator+1);
	}

}
