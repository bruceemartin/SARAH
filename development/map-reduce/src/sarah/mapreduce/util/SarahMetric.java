package sarah.mapreduce.util;

/*
 * SarahMetrics represent something that is observed about a data set.
 * SarahMetrics have a name and a description.
 * SarahMetrics can be longs, doubles or strings.
 * SarahMetrics can be represented as counters, properties or values in files.
 */

public class SarahMetric {
	public String name;
	public String description;
	public enum Destination {File,Property,Counter};
	public enum Type {Long,Double,String};
	public Type type;
	public Destination destination;

	
	public SarahMetric(String theName, String theDescription, Type theType) {
		name = theName;
		description = theDescription;
		destination = Destination.Property;
		type = theType;
	}
	
	public SarahMetric(String theName, String theDescription, Type theType, Destination theDestination) {
		name = theName;
		description = theDescription;
		destination = theDestination;
		type = theType;
	}
}
