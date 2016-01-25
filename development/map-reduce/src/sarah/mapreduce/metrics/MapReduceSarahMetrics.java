package sarah.mapreduce.metrics;

import org.apache.hadoop.conf.Configuration;

import sarah.metrics.SarahMetrics;

public class MapReduceSarahMetrics extends SarahMetrics {

	public MapReduceSarahMetrics(Configuration conf)  {
		super(conf.get(SarahMetrics.sarahFunctions.name).split(","));
		// Provide values from MapReduce configuration if defined
		
		for (String function: sarahFunctions.getValue()) {
			String functionClassName = SarahMetrics.get().stringForF(sarahFunctionClass.name, function);
			sarahFunctionClass.setValue(function,conf.get(functionClassName));
			String functionOutputKeyClassName = SarahMetrics.get().stringForF(sarahOutputKeyForF.name, function);
			sarahOutputKeyForF.setValue(function,conf.get(functionOutputKeyClassName));
		}
		numberInputRecords = new CountedLongMapReduceMetric(numberInputRecords,"numberInputRecords",conf);
		inputDataSetMetrics[0] = numberInputRecords;
		
		sarahSampleSeed.setValue(conf.getLong(sarahSampleSeed.name,0));
		sarahTargetSampleFraction.setValue(conf.getDouble(sarahTargetSampleFraction.name, .01));
		sarahSampleFraction.setValue(conf.getDouble(sarahSampleFraction.name, .01));
		sarahSampleOutputFormat.setValue(conf.get(sarahSampleOutputFormat.name));
		sarahSampleOutputKeyClass.setValue(conf.get(sarahSampleOutputKeyClass.name));
		sarahSampleOutputValueClass.setValue(conf.get(sarahSampleOutputValueClass.name));
		numberSampleRecords = new CountedLongMapReduceMetric(numberSampleRecords,"numberSampleRecords",conf);
		sampleMetrics[6] = numberSampleRecords;
		
		numberOutputRecordsForF = new CountedLongMapReduceMetric(numberOutputRecordsForF,"numberOutputRecordsForF",conf);
		numberOutputKeysForF = new CountedLongMapReduceMetric(numberOutputKeysForF,"numberOutputKeysForF",conf);
		minSizeRecordForF = new CountedLongMapReduceMetric(minSizeRecordForF,"minSizeRecordForF",conf);
		maxSizeRecordForF = new CountedLongMapReduceMetric(maxSizeRecordForF,"maxSizeRecordForF",conf);
		totalSizeRecordsForF = new CountedLongMapReduceMetric(totalSizeRecordsForF,"totalSizeRecordsForF",conf);
		meanSizeRecordForF = new CountedDoubleMapReduceMetric(meanSizeRecordForF,"meanSizeRecordForF",conf);
		percentile25ForF = new CountedDoubleMapReduceMetric(percentile25ForF,"percentile25ForF",conf);
		percentile50ForF = new CountedDoubleMapReduceMetric(percentile50ForF,"percentile50ForF",conf);
		percentile75ForF = new CountedDoubleMapReduceMetric(percentile75ForF,"percentile75ForF",conf);
		
		sampleFunctionMetrics[0] = numberOutputRecordsForF;
		sampleFunctionMetrics[1] = numberOutputKeysForF;
		sampleFunctionMetrics[2] = minSizeRecordForF;
		sampleFunctionMetrics[3] = maxSizeRecordForF;
		sampleFunctionMetrics[4] = totalSizeRecordsForF;
		sampleFunctionMetrics[5] = meanSizeRecordForF;
		sampleFunctionMetrics[6] = percentile25ForF;
		sampleFunctionMetrics[7] = percentile50ForF;
		sampleFunctionMetrics[8] = percentile75ForF;
		
		numberOutputRecordsForK = new CountedLongMapReduceMetric(numberOutputRecordsForK,"numberOutputRecordsForK",conf);
		minSizeRecordForK = new CountedLongMapReduceMetric(minSizeRecordForK,"minSizeRecordForK",conf);
		maxSizeRecordForK = new CountedLongMapReduceMetric(maxSizeRecordForK,"maxSizeRecordForK",conf);
		totalSizeRecordsForK = new CountedLongMapReduceMetric(totalSizeRecordsForK,"totalSizeRecordsForK",conf);
		meanSizeRecordForK = new CountedDoubleMapReduceMetric(meanSizeRecordForK,"meanSizeRecordForK",conf);
		percentile25ForK = new CountedDoubleMapReduceMetric(percentile25ForK,"percentile25ForK",conf);
		percentile50ForK = new CountedDoubleMapReduceMetric(percentile50ForK,"percentile50ForK",conf);
		percentile75ForK = new CountedDoubleMapReduceMetric(percentile75ForK,"percentile75ForK",conf);
		
		sampleFunctionKeyMetrics[0] = numberOutputRecordsForK;
		sampleFunctionKeyMetrics[1] = minSizeRecordForK;
		sampleFunctionKeyMetrics[2] = maxSizeRecordForK;
		sampleFunctionKeyMetrics[3] = totalSizeRecordsForK;
		sampleFunctionKeyMetrics[4] = meanSizeRecordForK;
		sampleFunctionKeyMetrics[5] = percentile25ForK;
		sampleFunctionKeyMetrics[6] = percentile50ForK;
		sampleFunctionKeyMetrics[7] = percentile75ForK;
	
	}

}
