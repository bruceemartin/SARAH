package sarah.metrics;

public class SarahMetrics {
	// metrics describing functions being analyzed
	// The name of each metric is available statically as a SarahMetric
	// The metric objects are instantiated when and instance of SarahMetrics are instantiated.
	// The metric objects can be retried with get()
	
	
	public static SarahMetric<String[]> sarahFunctions = new SarahMetric<String[]>("sarah.functions","The functions applied to the generated sample.");  
	
	public SarahMetric<String> sarahToolName = new SarahMetric<String>("sarah.tool.name","The name of the tool that generated these statitistics");
	public SarahMetric<String> sarahToolVersion = new SarahMetric<String>("sarah.tool.version","The version of the tool that generated these statitistics");
	public SarahMetric<String> sarahDateOfGeneration = new SarahMetric<String>("sarah.date.generated","Date and time of statistic generation");
	public SarahMetric<?>[] toolMetrics = {sarahToolName,sarahToolVersion,sarahDateOfGeneration};
	
	public SarahMetric<String> sarahFunctionClass = new SarahMetric<String>("sarah.%func.class","The class that implements the %func function.");
	public SarahMetric<String> sarahOutputKeyForF = new SarahMetric<String>("sarah.%func.output.key.class","The class that implements the key output by the %func function.");
	public SarahMetric<?>[] functionMetrics = {sarahFunctionClass,sarahOutputKeyForF};
	
	// metrics describing input data set
	public CountedLongSarahMetric numberInputRecords = new CountedLongSarahMetric("sarah.input.number.records","The number of records in the input data set.");	
	public CountedLongSarahMetric[] inputDataSetMetrics = {numberInputRecords};
	
	// metrics describing generated sample
	public SarahMetric<Long> sarahSampleSeed = new SarahMetric<Long>("sarah.sample.seed","The seed used to generate the random sample.");	
	public SarahMetric<Double> sarahTargetSampleFraction = new SarahMetric<Double>("sarah.target.sample.fraction","The desired fraction of records included in the generated random sample.");
	public SarahMetric<Double> sarahSampleFraction = new SarahMetric<Double>("sarah.sample.fraction","Fraction of the records actually included in the generated random sample.");
	public SarahMetric<String> sarahSampleOutputFormat = new SarahMetric<String>("sarah.sample.outputformat.class","Hadoop file output format of the generated random sample.");
	public SarahMetric<String> sarahSampleOutputKeyClass= new SarahMetric<String>("sarah.sample.output.key.class","The class that implements the key in the generated random sample.");
	public SarahMetric<String> sarahSampleOutputValueClass = new SarahMetric<String>("sarah.sample.output.value.class","The class that implements the record in the generated random sample.");
	public CountedLongSarahMetric numberSampleRecords = new CountedLongSarahMetric("sarah.sample.number.records","The number of records in the generated random sample.");
	public SarahMetric<?>[] sampleMetrics = {
												sarahSampleSeed,
												sarahTargetSampleFraction,
												sarahSampleFraction,
												sarahSampleOutputFormat,
												sarahSampleOutputKeyClass,
												sarahSampleOutputValueClass,
												numberSampleRecords
											  };
	
	// metrics describing generated sample per function
	public CountedLongSarahMetric numberOutputRecordsForF = new CountedLongSarahMetric("sarah.sample.%func.number.records","The number of records resulting from applying %func to the generated random sample.");
	public CountedLongSarahMetric numberOutputKeysForF = new CountedLongSarahMetric("sarah.sample.%func.number.keys","The number of distinct keys resulting from applying %func to the generated random sample.");
	public CountedLongSarahMetric minSizeRecordForF = new CountedLongSarahMetric("sarah.sample.%func.minsize.record","The size of the smallest record resulting from applying %func to the generated sample.");
	public CountedLongSarahMetric maxSizeRecordForF = new CountedLongSarahMetric("sarah.sample.%func.maxsize.record","The size of the largest record resulting from applying %func to the generated sample.");
	public CountedLongSarahMetric totalSizeRecordsForF = new CountedLongSarahMetric("sarah.sample.%func.totalsize.records","The total size of the records resulting from applying %func to the generated sample.");	
	public CountedDoubleSarahMetric meanSizeRecordForF = new CountedDoubleSarahMetric("sarah.sample.%func.mean.size.record","The mean record size resulting from applying %func to the generated random sample.");
	public CountedDoubleSarahMetric percentile25ForF = new CountedDoubleSarahMetric("sarah.sample.%func.percentile25.size.record","The size of the record at the 25% percentile resulting from applying %func to the generated sample.");
	public CountedDoubleSarahMetric percentile50ForF = new CountedDoubleSarahMetric("sarah.sample.%func.percentile50.size.record","The size of the record at the 50% percentile resulting from applying %func to the generated sample.");
	public CountedDoubleSarahMetric percentile75ForF = new CountedDoubleSarahMetric("sarah.sample.%func.percentile75.size.records","The size of the record at the 75% percentile resulting from applying %func to the generated sample.");	
	
	
	public SarahMetric<?>[] sampleFunctionMetrics = {
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
	public CountedLongSarahMetric numberOutputRecordsForK = new CountedLongSarahMetric("sarah.sample.%func.number.records","The number of records resulting from applying %func to the generated random sample.");
	public CountedLongSarahMetric minSizeRecordForK = new CountedLongSarahMetric("sarah.sample.%func.minsize.record","The size of the smallest record resulting from applying %func to the generated sample.");
	public CountedLongSarahMetric maxSizeRecordForK = new CountedLongSarahMetric("sarah.sample.%func.maxsize.record","The size of the largest record resulting from applying %func to the generated sample.");
	public CountedLongSarahMetric totalSizeRecordsForK = new CountedLongSarahMetric("sarah.sample.%func.totalsize.records","The total size of the records resulting from applying %func to the generated sample.");	
	public CountedDoubleSarahMetric meanSizeRecordForK = new CountedDoubleSarahMetric("sarah.sample.%func.mean.size.record","The mean record size resulting from applying %func to the generated random sample.");
	public CountedDoubleSarahMetric percentile25ForK = new CountedDoubleSarahMetric("sarah.sample.%func.percentile25.size.record","The size of the record at the 25% percentile resulting from applying %func to the generated sample.");
	public CountedDoubleSarahMetric percentile50ForK = new CountedDoubleSarahMetric("sarah.sample.%func.percentile50.size.record","The size of the record at the 50% percentile resulting from applying %func to the generated sample.");
	public CountedDoubleSarahMetric percentile75ForK = new CountedDoubleSarahMetric("sarah.sample.%func.percentile75.size.records","The size of the record at the 75% percentile resulting from applying %func to the generated sample.");
	
	public SarahMetric<?>[] sampleFunctionKeyMetrics = {
												numberOutputRecordsForK,
												minSizeRecordForK,
												maxSizeRecordForK,
												totalSizeRecordsForK,
												meanSizeRecordForK,
												percentile25ForK,
												percentile50ForK,
												percentile75ForK
											};
	public SarahMetric<?>[] extendedMetrics = {};
	

	// Function to generate parameterized metric names.
	public String stringForF(String pattern, String functionName) {
		return pattern.replace("%func", functionName);
	}
	
	
	
	
	
	public SarahMetrics(String[] functions) {
		sarahFunctions.setValue(functions);
		singleton = this;
	}
	

	private static SarahMetrics singleton;
	
	public static SarahMetrics get() {
		return singleton;
	}




	
}
