package sarah.mapreduce.metrics;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import sarah.metrics.CountedDoubleSarahMetric;
import sarah.metrics.SarahMetrics;

// Implements double counters as longs since Hadoop does not support double counters.

public class CountedDoubleMapReduceMetric extends CountedDoubleSarahMetric {
	
	public CountedDoubleMapReduceMetric(CountedDoubleSarahMetric metric, Configuration conf) {
		super(metric.name,metric.description);
		if (!parameterizedByFunction()) {
			String value = conf.get(name);
			if (value!=null) {
				setValue(conf.getDouble(name, 0.0));
			}
		} else {
			for (String function: SarahMetrics.sarahFunctions.getValue()) {
				String propertyName = SarahMetrics.get().stringForF(name, function);
				String propertyValue = conf.get(propertyName);
				if (propertyValue!=null) {
					setValue(function,conf.getDouble(propertyValue,0.0));
				} else {
					setValue(function,0.0);
				}
					
			}
		}
	}

	

	private static long conversionFactor=1000;
	
	@Override
	public void setValue(Double theValue) {
		super.setValue(theValue *  conversionFactor);
	}

	@Override
	public Double getValue() {
		return (super.getValue() / conversionFactor);
	}


	@Override
	public void increment(long amount) throws IOException {
		super.increment(amount);
		MapReduceMetricService.service.getCounter(name).increment(amount * conversionFactor);
	}
	
	@Override
	public void increment(String functionName, long amount) throws IOException {
		super.increment(functionName,amount);
		MapReduceMetricService.service.getCounter(functionName, name).increment(amount * conversionFactor);
	}

}
