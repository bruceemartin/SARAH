package sarah.mapreduce.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.io.Writable;

public class StatsForFunctionAndKey implements Writable {
	public long numberOutputRecordsForKey;
	public double minSizeRecordForKey;
	public double maxSizeRecordForKey;
	public double totalSizeRecordsForKey;
	public double meanSizeRecordForKey;
	public double quantile25ForKey;
	public double quantile50ForKey;
	public double quantile75ForKey;

	public StatsForFunctionAndKey(DescriptiveStatistics statsForFunctionAndKey) {
		numberOutputRecordsForKey = statsForFunctionAndKey.getN();
		minSizeRecordForKey = statsForFunctionAndKey.getMin();
		maxSizeRecordForKey = statsForFunctionAndKey.getMax();
		totalSizeRecordsForKey = statsForFunctionAndKey.getSum();
		meanSizeRecordForKey = statsForFunctionAndKey.getMean();
		quantile25ForKey = statsForFunctionAndKey.getPercentile(25);
		quantile50ForKey = statsForFunctionAndKey.getPercentile(50);
		quantile75ForKey = statsForFunctionAndKey.getPercentile(75);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		numberOutputRecordsForKey = in.readLong();
		minSizeRecordForKey = in.readDouble();
		maxSizeRecordForKey = in.readDouble();
		totalSizeRecordsForKey = in.readDouble();
		meanSizeRecordForKey = in.readDouble();
		quantile25ForKey = in.readDouble();
		quantile50ForKey = in.readDouble();
		quantile75ForKey = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(numberOutputRecordsForKey);
		out.writeDouble(minSizeRecordForKey);
		out.writeDouble(maxSizeRecordForKey);
		out.writeDouble(totalSizeRecordsForKey);
		out.writeDouble(meanSizeRecordForKey);
		out.writeDouble(quantile25ForKey);
		out.writeDouble(quantile50ForKey);
		out.writeDouble(quantile75ForKey);
	}

}
