package sarah.metrics;

import java.io.IOException;


public class CountedLongSarahMetric extends SarahMetric<Long> {

	public CountedLongSarahMetric(String theName, String theDescription) {
		super(theName, theDescription);
		setValue(0L);
	}

	public void increment(long amount) throws IOException {
		setValue(getValue()+amount);
	}
	
	public void increment(String functionName, long amount) throws IOException {
		setValue(functionName,getValue(functionName)+amount);
	}
	
}
