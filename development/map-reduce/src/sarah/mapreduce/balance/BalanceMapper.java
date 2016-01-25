package sarah.mapreduce.balance;


import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import sarah.mapreduce.metrics.FunctionKey;
import sarah.mapreduce.metrics.MapReduceCounters;
import sarah.metrics.CountedLongSarahMetric;
import sarah.metrics.SarahMetrics;
import sarah.metrics.StatsForFunctionAndKey;



/* This mapper generates artifacts to balance reducers on samples of intermediate data sets.
 * A mapper is given for each key, the value counts produced by a single function in key sorted order.  
 * The counts are given to the mapper one at a time in a single map call.
 * The mapper balances the values into k buckets and stores the keys defining the intervals in a Sequence
 * File that serves as input to the TotalOrderPartitioner and a human readable text file of intervals.
 *  
 */


public class BalanceMapper<KEY> extends Mapper<FunctionKey, StatsForFunctionAndKey, KEY,NullWritable> {
	// Set in setup from incoming properties
	private int preferredPartitionSize;  
	private float sampleSize;
	
	private SequenceFile.Writer writer = null;
	private FSDataOutputStream textFile = null;
	MultipleOutputs<KEY,NullWritable> multipleOutputs;
	
	private String functionName; // The same for all keys passed to this mapper
	private FunctionKey prevKey=new FunctionKey();
	private long prevProposedPartitionSize=0;
		
	private NullWritable nullValue = NullWritable.get();
	private boolean foundPartition = false;
	
	// There are three possible states.
	// first: prevKey is not set, key is the first key
	// processing: prevKey is the previous key, key is the current key
	// post: prevKey is the previous key, key is not set.  postMap is called from cleanup.
	@Override
	protected void map(FunctionKey key, StatsForFunctionAndKey stats, Context context)
			throws IOException, InterruptedException {

		if (!prevKey.isSet()) {
			firstMap(key, stats, context);
		} else {
			processingMap(key, stats, context);
		}
		prevKey.setTo(key);
	}

	private void firstMap(FunctionKey key, StatsForFunctionAndKey stats,Context context) {
		long proposedPartitionSize = (long) (stats.numberOutputRecordsForKey / sampleSize);
		functionName = key.getFunctionName();
		foundPartition = (proposedPartitionSize>=preferredPartitionSize);
		prevProposedPartitionSize = proposedPartitionSize;
	}

	private void processingMap(FunctionKey key, StatsForFunctionAndKey stats,Context context) throws IOException, InterruptedException {
		if (foundPartition) {
			writePartition(prevProposedPartitionSize);
			writeInterval(prevKey,prevProposedPartitionSize);
			prevProposedPartitionSize=0;
		}
		// Consider the effect of this key
		long proposedPartitionSize = prevProposedPartitionSize + (long) (stats.numberOutputRecordsForKey / sampleSize);
		if ((proposedPartitionSize >= preferredPartitionSize) && (prevProposedPartitionSize >= .5*preferredPartitionSize)) {
			// The current key is so large that there is no point in adding to it, even if it means
			// creating a small partition.
			writePartition(prevProposedPartitionSize);
			writeInterval(prevKey,prevProposedPartitionSize);
			// We processed the previous partition, so the proposedPartition shouldn't include it.
			proposedPartitionSize = proposedPartitionSize - prevProposedPartitionSize;
		}
		foundPartition = (proposedPartitionSize>=preferredPartitionSize);
		prevProposedPartitionSize = proposedPartitionSize;
	}
	
	// called to write the final partition
	private void postMap(Context context) throws IOException, InterruptedException {
		if (prevProposedPartitionSize>0) {
			writePartition(prevProposedPartitionSize);
		}
	}

	private void writePartition(long partitionSize) throws IOException, InterruptedException {
		CountedLongSarahMetric recommendedReducers = BalanceSarahMetrics.get().recommendedNumberReducersForF;
		recommendedReducers.increment(functionName,1L);
		multipleOutputs.write(functionName+"txt", partitionSize+" estimated records in partition.",nullValue,functionName+"-txt/interval");
	}

	private void writeInterval(FunctionKey key,long partitionSize) throws IOException, InterruptedException {
		multipleOutputs.write(functionName, key.getKey(), nullValue,functionName+"/interval");
		multipleOutputs.write(functionName+"txt", key.getKey().toString(),nullValue,functionName+"-txt/interval");
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// Get the sample size from the configuration
		sampleSize = context.getConfiguration().getFloat(SarahMetrics.get().sarahSampleFraction.name, 0);
		
		preferredPartitionSize = context.getConfiguration().getInt("sarah.partition.preferredSize", 100000);
		new BalanceMetricService(context);
		multipleOutputs = new MultipleOutputs<KEY, NullWritable>(context);
		super.setup(context);
	}
	

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		postMap(context);
		if (writer!=null) writer.close();
		if (textFile!=null) textFile.close();
		multipleOutputs.close();
		super.cleanup(context);
		MapReduceCounters.addSarahMetricsToCounters();
	}


	
	

}
