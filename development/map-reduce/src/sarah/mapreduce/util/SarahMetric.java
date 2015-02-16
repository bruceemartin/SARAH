package sarah.mapreduce.util;

public class SarahMetric {
	public String name;
	public String description;
	public enum Destination {File,Property,Counter};
	public Destination destination;

	
	public SarahMetric(String theName, String theDescription) {
		name = theName;
		description = theDescription;
		destination = Destination.Property;
	}
	
	public SarahMetric(String theName, String theDescription, Destination theDestination) {
		name = theName;
		description = theDescription;
		destination = theDestination;
	}
}
