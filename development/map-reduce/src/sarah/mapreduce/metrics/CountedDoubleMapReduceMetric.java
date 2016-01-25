package sarah.mapreduce.metrics;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;

import sarah.metrics.CountedDoubleSarahMetric;
import sarah.metrics.SarahMetricServiceBase;
import sarah.metrics.SarahMetrics;

// Implements double counters as longs since Hadoop does not support double counters.

public class CountedDoubleMapReduceMetric extends CountedDoubleSarahMetric implements CountedMapReduceMetric {

	private String counterId="";
	private static long conversionFactor=1000;
	
	public CountedDoubleMapReduceMetric(CountedDoubleSarahMetric metric, String counterId, Configuration conf) {
		super(metric.name,metric.description);
		if (!parameterizedByFunction()) {
			setValue(conf.getDouble(name, 0.0));
		} else {
			for (String function: SarahMetrics.sarahFunctions.getValue()) {
				String propertyName = SarahMetrics.get().stringForF(name, function);
				setValue(function,conf.getDouble(propertyName,0.0));				
			}
		}
		this.counterId = counterId;
		MapReduceCounters.registerCountedMetric(this,counterId);
	}
	
	public void setCounterValues( ) throws IOException {
		if (parameterizedByFunction()) {
			for (String functionName: functionNames()) {
				Counter counter = MapReduceCounters.context.getCounter(SarahMetricServiceBase.sarahTitle, counterId+"/"+functionName);
				counter.increment((long)(getValue(functionName)*conversionFactor));
			}
		} else {
			Counter counter = MapReduceCounters.context.getCounter(SarahMetricServiceBase.sarahTitle,counterId);
			counter.increment((long)(getValue()*conversionFactor));
		}
	}

	@Override
	public void setValueFromCounter(Counter counter) {
		if (parameterizedByFunction()) {
			setValue(CountedLongMapReduceMetric.functionName(counter),((double)counter.getValue())/conversionFactor);
		} else {
			setValue(((double)counter.getValue())/conversionFactor);
		}
	}
}
