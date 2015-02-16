package sarah.mapreduce.statistics;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import sarah.mapreduce.util.FunctionKey;
import sarah.mapreduce.util.SarahMetricNames;



// calculates one partition per function using function name in key and sarah.functions list
public class MultiFunctionPartitioner extends Partitioner<FunctionKey, LongWritable> implements Configurable {
	private String[] functions = null;
	private Configuration configuration = null;
	@Override
	public int getPartition(FunctionKey key, LongWritable value, int numberOfReducers) {
		for (int i=0; i<functions.length; i++) {
			if (key.getFunctionName().equals(functions[i])) {
				return i;
			}
		}
		return 0;
	}
	@Override
	public Configuration getConf() {
		return configuration;
	}
	@Override
	public void setConf(Configuration conf) {
		configuration = conf;
		functions = conf.getStrings(SarahMetricNames.sarahFunctions.name);
	}

}
