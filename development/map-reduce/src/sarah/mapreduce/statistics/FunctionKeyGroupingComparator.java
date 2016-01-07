package sarah.mapreduce.statistics;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import sarah.mapreduce.metrics.FunctionKey;

public class FunctionKeyGroupingComparator extends WritableComparator {

	protected FunctionKeyGroupingComparator() {
		super(FunctionKey.class, true);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		FunctionKey a = (FunctionKey)w1;
		FunctionKey b = (FunctionKey)w2;
		int compareNames = a.getFunctionName().compareTo(b.getFunctionName());
		if (compareNames != 0) {
			return compareNames;
		}
		return a.getKey().compareTo(b.getKey());
		
	}	
	
}
