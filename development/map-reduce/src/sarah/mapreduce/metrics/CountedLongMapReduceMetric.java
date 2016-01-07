package sarah.mapreduce.metrics;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;

import sarah.metrics.CountedLongSarahMetric;
import sarah.metrics.SarahMetrics;

public class CountedLongMapReduceMetric extends CountedLongSarahMetric {

	public CountedLongMapReduceMetric(CountedLongSarahMetric metric, Configuration conf) {
		super(metric.name,metric.description);
		if (!parameterizedByFunction()) {
			String value = conf.get(name);
			if (value!=null) {
				setValue(conf.getLong(name, 0));
			}
		} else {
			for (String function: SarahMetrics.sarahFunctions.getValue()) {
				String propertyName = SarahMetrics.get().stringForF(name, function);
				String propertyValue = conf.get(propertyName);
				if (propertyValue!=null) {
					setValue(function,conf.getLong(propertyValue,0));
				} else {
					setValue(function,0L);
				}
			}
		}
	}
	

	@Override
	public void increment(long amount) throws IOException {
		super.increment(amount);
		MapReduceMetricService.service.getCounter(name).increment(amount);
	}
	
	@Override
	public void increment(String functionName, long amount) throws IOException {
		super.increment(functionName,amount);
		MapReduceMetricService theService = MapReduceMetricService.service;
		Counter theCounter = theService.getCounter(functionName, name);
		theCounter.increment(amount);
	}
	
	
}
