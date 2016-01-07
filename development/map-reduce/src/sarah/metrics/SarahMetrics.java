package sarah.mapreduce.util;

public class SarahMetricNames {
	// metrics describing functions being analyzed
	private static SarahMetric.Type aString = SarahMetric.Type.String;
	private static SarahMetric.Type aLong = SarahMetric.Type.Long;
	private static SarahMetric.Type aDouble = SarahMetric.Type.Double;
	private static SarahMetric.Destination asProperty = SarahMetric.Destination.Property;
	private static SarahMetric.Destination asCounter = SarahMetric.Destination.Counter;
	private static SarahMetric.Destination file = SarahMetric.Destination.File;
	
	public static SarahMetric sarahFunctions = 
			new SarahMetric("sarah.functions","The functions applied to the generated sample.",aString,asProperty);  
	public static SarahMetric sarahFunctionClass = 
			new SarahMetric("sarah.%func.class","The class that implements the %func function.",aString,asProperty);
	public static SarahMetric sarahOutputKeyForF = 
			new SarahMetric("sarah.%func.output.key.class","The class that implements the key output by the %func function.",aString,asProperty);
	public static SarahMetric[] functionMetrics = 
			{sarahFunctionClass,sarahOutputKeyForF};
	
	// metrics describing input data set
	public static SarahMetric numberInputRecords = 
			new SarahMetric("sarah.input.number.records","The number of records in the input data set.",aLong,asCounter);
	public static SarahMetric[] inputDataSetMetrics = 
			{numberInputRecords};
	
	// metrics describing generated sample
	public static SarahMetric sarahSampleSeed = 
			new SarahMetric("sarah.sample.seed","The seed used to generate the random sample.",aDouble,asProperty);	
	public static SarahMetric sarahTargetSampleFraction = 
			new SarahMetric("sarah.target.sample.fraction","The desired fraction of records included in the generated random sample.",aDouble,asProperty);
	public static SarahMetric sarahSampleFraction = 
			new SarahMetric("sarah.sample.fraction","Fraction of the records actually included in the generated random sample.",aDouble,asProperty);
	public static SarahMetric sarahSampleOutputFormat = 
			new SarahMetric("sarah.sample.outputformat.class","Hadoop file output format of the generated random sample.",aString,asProperty);
	public static SarahMetric sarahSampleOutputKeyClass= 
			new SarahMetric("sarah.sample.output.key.class","The class that implements the key in the generated random sample.",aString,asProperty);
	public static SarahMetric sarahSampleOutputValueClass = 
			new SarahMetric("sarah.sample.output.value.class","The class that implements the record in the generated random sample.",aString,asProperty);
	public static SarahMetric numberSampleRecords = 
			new SarahMetric("sarah.sample.number.records","The number of records in the generated random sample.",aLong,asCounter);
	public static SarahMetric[] sampleMetrics = {
												sarahSampleSeed,
												sarahTargetSampleFraction,
												sarahSampleFraction,
												sarahSampleOutputFormat,
												sarahSampleOutputKeyClass,
												sarahSampleOutputValueClass,
												numberSampleRecords
											  };
	
	// metrics describing generated sample per function
	public static SarahMetric numberOutputRecordsForF = 
			new SarahMetric("sarah.sample.%func.number.records","The number of records resulting from applying %func to the generated random sample.",aLong,asCounter);
	public static SarahMetric numberOutputKeysForF = 
			new SarahMetric("sarah.sample.%func.number.keys","The number of distinct keys resulting from applying %func to the generated random sample.",aLong,asCounter);
	public static SarahMetric minSizeRecordForF = 
			new SarahMetric("sarah.sample.%func.minsize.record","The size of the smallest record resulting from applying %func to the generated sample.",aLong,asCounter);
	public static SarahMetric maxSizeRecordForF = 
			new SarahMetric("sarah.sample.%func.maxsize.record","The size of the largest record resulting from applying %func to the generated sample.",aLong,asCounter);
	public static SarahMetric totalSizeRecordsForF = 
			new SarahMetric("sarah.sample.%func.totalsize.records","The total size of the records resulting from applying %func to the generated sample.",aLong,asCounter);	
	
	public static SarahMetric meanSizeRecordForF = 
			new SarahMetric("sarah.sample.%func.mean.size.record","The mean record size resulting from applying %func to the generated random sample.",aDouble,asCounter);
	public static SarahMetric percentile25ForF = 
			new SarahMetric("sarah.sample.%func.percentile25.size.record","The size of the record at the 25% percentile resulting from applying %func to the generated sample.",aDouble,asCounter);
	public static SarahMetric percentile50ForF = 
			new SarahMetric("sarah.sample.%func.percentile50.size.record","The size of the record at the 50% percentile resulting from applying %func to the generated sample.",aDouble,asCounter);
	public static SarahMetric percentile75ForF = 
			new SarahMetric("sarah.sample.%func.percentile75.size.records","The size of the record at the 75% percentile resulting from applying %func to the generated sample.",aDouble,asCounter);	
	
	public static SarahMetric[] sampleFunctionMetrics = {
												numberOutputRecordsForF,
												numberOutputKeysForF,
												minSizeRecordForF,
												maxSizeRecordForF,
												totalSizeRecordsForF,
												meanSizeRecordForF,
												percentile25ForF,
												percentile50ForF,
												percentile75ForF
											};
	// metrics describing generated sample per key per function
	public static SarahMetric numberOutputRecordsForK = 
			new SarahMetric("sarah.sample.%func.number.records","The number of records resulting from applying %func to the generated random sample.",aLong,file);
	public static SarahMetric minSizeRecordForK = 
			new SarahMetric("sarah.sample.%func.minsize.record","The size of the smallest record resulting from applying %func to the generated sample.",aLong,file);
	public static SarahMetric maxSizeRecordForK = 
			new SarahMetric("sarah.sample.%func.maxsize.record","The size of the largest record resulting from applying %func to the generated sample.",aLong,file);
	public static SarahMetric totalSizeRecordsForK = 
			new SarahMetric("sarah.sample.%func.totalsize.records","The total size of the records resulting from applying %func to the generated sample.",aLong,file);	
	
	public static SarahMetric meanSizeRecordForK = 
			new SarahMetric("sarah.sample.%func.mean.size.record","The mean record size resulting from applying %func to the generated random sample.",aDouble,file);
	public static SarahMetric percentile25ForK = 
			new SarahMetric("sarah.sample.%func.percentile25.size.record","The size of the record at the 25% percentile resulting from applying %func to the generated sample.",aDouble,file);
	public static SarahMetric percentile50ForK = 
			new SarahMetric("sarah.sample.%func.percentile50.size.record","The size of the record at the 50% percentile resulting from applying %func to the generated sample.",aDouble,file);
	public static SarahMetric percentile75ForK = 
			new SarahMetric("sarah.sample.%func.percentile75.size.records","The size of the record at the 75% percentile resulting from applying %func to the generated sample.",aDouble,file);	
	
	public static SarahMetric[] sampleFunctionKeyMetrics = {
												numberOutputRecordsForK,
												minSizeRecordForK,
												maxSizeRecordForK,
												totalSizeRecordsForK,
												meanSizeRecordForK,
												percentile25ForK,
												percentile50ForK,
												percentile75ForK
											};
	
	// Function to generate parameterized metric names.
	public static String stringForF(String pattern, String functionName) {
		return pattern.replace("%func", functionName);
	}


}
