package sarah.mapreduce.statistics;


import java.io.IOException;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import sarah.mapreduce.util.FunctionKey;
import sarah.mapreduce.util.SarahMapReduceMetrics;
import sarah.mapreduce.util.SarahMetricNames;
import sarah.mapreduce.util.StatsForFunctionAndKey;


/* This reducer generates statistics on samples of intermediate data sets that can be
 * processed in a single reducer.
 */

public class MultiFunctionReducer<KEY> extends Reducer<FunctionKey, LongWritable,KEY,StatsForFunctionAndKey>  {	
	private MultipleOutputs<KEY, StatsForFunctionAndKey> multipleOutputs;
	private SarahMapReduceMetrics sarahMetrics=null;
	private DescriptiveStatistics statsForFunction;
	private DescriptiveStatistics statsForFunctionAndKey;
	
	FunctionKey prevKey = new FunctionKey();
	
	/* Since the key is a composite of (Function,Key,RecordSize), a reduce call essentially
	 * has all of the records of the same size produced by a function for the key.
	 * The reducer itself is dedicated to a single function.  The reducer calculates the statistics
	 * for the function and the function and key.  To do this, the reduce call first checks to see 
	 * if the previous function key is different.  If it is, statistics are output for the previous function.key from simply iterates over
	 * the values and increments the statistics that are counting records and updates min and max
	 * record sizes.
	 * 
	 */
	@Override
	protected void reduce(FunctionKey key, Iterable<LongWritable> values,Context context)
			throws IOException, InterruptedException {

		checkNextFunctionKey(key);
		long recordSize = key.getRecordSize();
		for (LongWritable value: values) {
			long numberRecordsOfSize=value.get();	
			for (long i=0; i<numberRecordsOfSize; i++) {
				statsForFunctionAndKey.addValue(recordSize);
				statsForFunction.addValue(recordSize);
			}
		}
		if (prevKey.differentKey(key)) {
			prevKey.setTo(key);
		}
	}
	
	// checkNextFunctionKey detects when a new key is being processed and records per key statistics  
	private void checkNextFunctionKey(FunctionKey key) throws IOException, InterruptedException {
		if (prevKey.isSet() && prevKey.differentKey(key)) {
			sarahMetrics.increment(SarahMetricNames.numberOutputKeysForF,prevKey.getFunctionName(), 1L);
						
			multipleOutputs.write(prevKey.getFunctionName(), prevKey, new StatsForFunctionAndKey(statsForFunctionAndKey),
					"functions/" + prevKey.getFunctionName() + "/part");
			statsForFunctionAndKey.clear();
		}

	}
	
	// cleanup reports function statistics.  All of the records produced by the function have been processed.
	// because there is 1 reducer per function.
	@Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
		sarahMetrics.setLongValue(SarahMetricNames.numberOutputRecordsForF, prevKey.getFunctionName(),statsForFunction.getN());
		sarahMetrics.setLongValue(SarahMetricNames.minSizeRecordForF, prevKey.getFunctionName(), (long)statsForFunction.getMin());
		sarahMetrics.setLongValue(SarahMetricNames.maxSizeRecordForF, prevKey.getFunctionName(), (long)statsForFunction.getMax());
		sarahMetrics.setLongValue(SarahMetricNames.totalSizeRecordsForF, prevKey.getFunctionName(),(long)statsForFunction.getSum());
		sarahMetrics.setDoubleValue(SarahMetricNames.meanSizeRecordForF, prevKey.getFunctionName(), statsForFunction.getMean());
		sarahMetrics.setDoubleValue(SarahMetricNames.percentile25ForF, prevKey.getFunctionName(), statsForFunction.getPercentile(25));
		sarahMetrics.setDoubleValue(SarahMetricNames.percentile50ForF, prevKey.getFunctionName(), statsForFunction.getPercentile(50));
		sarahMetrics.setDoubleValue(SarahMetricNames.percentile75ForF, prevKey.getFunctionName(), statsForFunction.getPercentile(75));
        multipleOutputs.close();
    }

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// Reducer has two outputs -- the sample and the distribution/statistics.
		multipleOutputs = new MultipleOutputs<KEY, StatsForFunctionAndKey>(context);
		sarahMetrics = new SarahMapReduceMetrics(context);
		statsForFunction = new DescriptiveStatistics();
		statsForFunctionAndKey = new DescriptiveStatistics();
		super.setup(context);
	}
}
