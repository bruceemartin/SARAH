package sarah.metrics;

import java.io.IOException;


public class CountedDoubleSarahMetric extends SarahMetric<Double> {

	public CountedDoubleSarahMetric(String theName, String theDescription) {
		super(theName, theDescription);
		setValue(0.0);
	}

	public void increment(long amount) throws IOException {
		setValue(getValue()+amount);
	}
	
	public void increment(String functionName, long amount) throws IOException {
		setValue(functionName,getValue(functionName)+amount);
	}
	
}
