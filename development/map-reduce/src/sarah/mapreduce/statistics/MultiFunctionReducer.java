package sarah.mapreduce.statistics;


import java.io.IOException;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import sarah.mapreduce.metrics.FunctionKey;
import sarah.mapreduce.metrics.MapReduceCounters;
import sarah.mapreduce.metrics.MapReduceMetricService;
import sarah.metrics.SarahMetrics;
import sarah.metrics.StatsForFunctionAndKey;


/* This reducer generates statistics on samples of intermediate data sets that can be
 * processed in a single reducer.
 */

public class MultiFunctionReducer<KEY> extends Reducer<FunctionKey, LongWritable,KEY,StatsForFunctionAndKey>  {	
	private MultipleOutputs<KEY, StatsForFunctionAndKey> multipleOutputs;
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
			SarahMetrics.get().numberOutputKeysForF.increment(prevKey.getFunctionName(),1L);	
			multipleOutputs.write(prevKey.getFunctionName(), prevKey, new StatsForFunctionAndKey(statsForFunctionAndKey),
					"functions/" + prevKey.getFunctionName() + "/part");
			statsForFunctionAndKey.clear();
		}

	}
	
	// cleanup reports function statistics.  All of the records produced by the function have been processed.
	// because there is 1 reducer per function.

    protected void cleanup(Context context) throws IOException, InterruptedException {
		SarahMetrics.get().numberOutputRecordsForF.setValue(prevKey.getFunctionName(),statsForFunction.getN());
		SarahMetrics.get().minSizeRecordForF.setValue(prevKey.getFunctionName(), (long)statsForFunction.getMin());
		SarahMetrics.get().maxSizeRecordForF.setValue(prevKey.getFunctionName(), (long)statsForFunction.getMax());
		SarahMetrics.get().totalSizeRecordsForF.setValue(prevKey.getFunctionName(),(long)statsForFunction.getSum());
		SarahMetrics.get().meanSizeRecordForF.setValue(prevKey.getFunctionName(), statsForFunction.getMean());
		SarahMetrics.get().percentile25ForF.setValue(prevKey.getFunctionName(), statsForFunction.getPercentile(25));
		SarahMetrics.get().percentile50ForF.setValue(prevKey.getFunctionName(), statsForFunction.getPercentile(50));
		SarahMetrics.get().percentile75ForF.setValue(prevKey.getFunctionName(), statsForFunction.getPercentile(75));
        multipleOutputs.close();
        MapReduceCounters.addSarahMetricsToCounters();
    }

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// Reducer has two outputs -- the sample and the distribution/statistics.
		multipleOutputs = new MultipleOutputs<KEY, StatsForFunctionAndKey>(context);
		new MapReduceMetricService(context);
		statsForFunction = new DescriptiveStatistics();
		statsForFunctionAndKey = new DescriptiveStatistics();
		super.setup(context);
	}
}
