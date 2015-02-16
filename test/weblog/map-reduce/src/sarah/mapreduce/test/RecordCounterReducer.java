package sarah.mapreduce.test;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;


/* This reducer counts all records passed to it.
 */


public class RecordCounterReducer<KEYIN extends WritableComparable<?>,VALUEIN> extends
		Reducer<KEYIN,VALUEIN,Text,IntWritable> {

	
	private int numRecords;

	@Override
	protected void reduce(KEYIN key, Iterable<VALUEIN> values,Context context)
			throws IOException, InterruptedException {
		for (@SuppressWarnings("unused") VALUEIN value: values) {
			numRecords++;
		}
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		numRecords=0;
		super.setup(context);
	}
	

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		
		context.write(new Text(Context.TASK_ID), new IntWritable(numRecords));
		super.cleanup(context);
	}

}
