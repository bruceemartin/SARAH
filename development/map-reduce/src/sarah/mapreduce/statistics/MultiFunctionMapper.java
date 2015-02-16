package sarah.mapreduce.statistics;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.security.Credentials;

import sarah.mapreduce.util.FunctionKey;
import sarah.mapreduce.util.SarahMapReduceMetrics;
import sarah.mapreduce.util.SarahMetricNames;


@SuppressWarnings("rawtypes")
public class MultiFunctionMapper<KEYIN, VALUEIN> extends
		Mapper<KEYIN, VALUEIN, WritableComparable, LongWritable> {
	
	private MultipleOutputs<KEYIN,VALUEIN> multipleOutputs;
	private SarahMapReduceMetrics sarahMetrics=null;

	public Context getMapContext(
			MapContext<KEYIN, VALUEIN, WritableComparable, LongWritable> mapContext) {
		return new Context(mapContext);
	}

	public class Context extends
			Mapper<KEYIN, VALUEIN,WritableComparable,Writable>.Context {

		protected MapContext<KEYIN, VALUEIN, WritableComparable, LongWritable> mapContext;
		private String currentFunctionName = "";
		private KEYIN currentKey;
		private VALUEIN currentValue;
		private boolean hasRecord;
		final private LongWritable one = new LongWritable(1);
		

		public Context(MapContext<KEYIN, VALUEIN, WritableComparable, LongWritable> mapContext) {
			this.mapContext = mapContext;
		}



		public void setForMapper(String function, KEYIN key, VALUEIN value) {
			currentFunctionName = function;
			currentKey = key;
			currentValue = value;
			hasRecord = true;
		}
		
		@Override
		public KEYIN getCurrentKey() throws IOException, InterruptedException {
			return currentKey;
		}

		@Override
		public VALUEIN getCurrentValue() throws IOException,
				InterruptedException {
			return currentValue;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (hasRecord) {
				hasRecord=false;
				return true;
			}
			return false;
		}
		
		
		@Override
		public void write(WritableComparable key, Writable value) throws IOException,
				InterruptedException {
			mapContext.write(FunctionKey.getSingletonFunctionKey(currentFunctionName,key,recordSize(value)), one);
		}

		private long recordSize(Writable value) throws IOException {
			if (value instanceof BinaryComparable) {
				return ((BinaryComparable)value).getLength();
			} else {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputStream dos = new DataOutputStream(baos);
				value.write(dos);
				return baos.toByteArray().length;
			}
		}


		@Override
		public InputSplit getInputSplit() {
			return mapContext.getInputSplit();
		}
		
		@Override
		public Counter getCounter(Enum<?> counterName) {
			return mapContext.getCounter(counterName);
		}

		@Override
		public Counter getCounter(String groupName, String counterName) {
			return mapContext.getCounter(groupName, counterName);
		}

		@Override
		public OutputCommitter getOutputCommitter() {
			return mapContext.getOutputCommitter();
		}

		@Override
		public String getStatus() {
			return mapContext.getStatus();
		}

		@Override
		public TaskAttemptID getTaskAttemptID() {
			return mapContext.getTaskAttemptID();
		}

		@Override
		public void setStatus(String msg) {
			mapContext.setStatus(msg);
		}

		@Override
		public Path[] getArchiveClassPaths() {
			return mapContext.getArchiveClassPaths();
		}

		@Override
		public String[] getArchiveTimestamps() {
			return mapContext.getArchiveTimestamps();
		}

		@Override
		public URI[] getCacheArchives() throws IOException {
			return mapContext.getCacheArchives();
		}

		@Override
		public URI[] getCacheFiles() throws IOException {
			return mapContext.getCacheArchives();
		}

		@Override
		public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
				throws ClassNotFoundException {
			return mapContext.getCombinerClass();
		}

		@Override
		public Configuration getConfiguration() {
			return mapContext.getConfiguration();
		}

		@Override
		public Path[] getFileClassPaths() {
			return mapContext.getFileClassPaths();
		}

		@Override
		public String[] getFileTimestamps() {
			return mapContext.getFileTimestamps();
		}

		@Override
		public RawComparator<?> getGroupingComparator() {
			return mapContext.getGroupingComparator();
		}

		@Override
		public Class<? extends InputFormat<?, ?>> getInputFormatClass()
				throws ClassNotFoundException {
			return mapContext.getInputFormatClass();
		}

		@Override
		public String getJar() {
			return mapContext.getJar();
		}

		@Override
		public JobID getJobID() {
			return mapContext.getJobID();
		}

		@Override
		public String getJobName() {
			return mapContext.getJobName();
		}

		@Override
		public boolean getJobSetupCleanupNeeded() {
			return mapContext.getJobSetupCleanupNeeded();
		}

		@Override
		@Deprecated
		public Path[] getLocalCacheArchives() throws IOException {
			return mapContext.getLocalCacheArchives();
		}

		@Override
		@Deprecated
		public Path[] getLocalCacheFiles() throws IOException {
			return mapContext.getLocalCacheFiles();
		}

		@Override
		public Class<?> getMapOutputKeyClass() {
			return mapContext.getMapOutputKeyClass();
		}

		@Override
		public Class<?> getMapOutputValueClass() {
			return mapContext.getMapOutputValueClass();
		}

		@Override
		public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
				throws ClassNotFoundException {
			return mapContext.getMapperClass();
		}

		@Override
		public int getMaxMapAttempts() {
			return mapContext.getMaxMapAttempts();
		}

		@Override
		public int getMaxReduceAttempts() {
			return mapContext.getMaxReduceAttempts();
		}

		@Override
		public int getNumReduceTasks() {
			return mapContext.getNumReduceTasks();
		}

		@Override
		public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
				throws ClassNotFoundException {
			return mapContext.getOutputFormatClass();
		}

		@Override
		public Class<?> getOutputKeyClass() {
			return mapContext.getOutputKeyClass();
		}

		@Override
		public Class<?> getOutputValueClass() {
			return mapContext.getOutputValueClass();
		}

		@Override
		public Class<? extends Partitioner<?, ?>> getPartitionerClass()
				throws ClassNotFoundException {
			return mapContext.getPartitionerClass();
		}

		@Override
		public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
				throws ClassNotFoundException {
			return mapContext.getReducerClass();
		}

		@Override
		public RawComparator<?> getSortComparator() {
			return mapContext.getSortComparator();
		}

		@Override
		@Deprecated
		public boolean getSymlink() {
			return mapContext.getSymlink();
		}

		@Override
		public Path getWorkingDirectory() throws IOException {
			return mapContext.getWorkingDirectory();
		}

		@Override
		public void progress() {
			mapContext.progress();
		}

		@Override
		public boolean getProfileEnabled() {
			return mapContext.getProfileEnabled();
		}

		@Override
		public String getProfileParams() {
			return mapContext.getProfileParams();
		}

		@Override
		public String getUser() {
			return mapContext.getUser();
		}

		@Override
		public Credentials getCredentials() {
			return mapContext.getCredentials();
		}

		@Override
		public boolean userClassesTakesPrecedence() {
			return mapContext.userClassesTakesPrecedence();
		}

		@Override
		public float getProgress() {
			return mapContext.getProgress();
		}

		@Override
		public RawComparator<?> getCombinerKeyGroupingComparator() {
			return mapContext.getCombinerKeyGroupingComparator();
		}

		@Override
		public IntegerRanges getProfileTaskRange(boolean arg0) {
			return mapContext.getProfileTaskRange(arg0);
		}

		@Override
		public boolean getTaskCleanupNeeded() {
			return mapContext.getTaskCleanupNeeded();
		}

	}
	
	private float targetSampleSize = .05F;
	private Random generator;
	private boolean nullSampleOutputKey = false;
	private String functions[];
	private Mapper<KEYIN, VALUEIN, WritableComparable, Writable> userMappers[];

	@SuppressWarnings("unchecked")
	@Override
	protected void setup(
			org.apache.hadoop.mapreduce.Mapper<KEYIN, VALUEIN, WritableComparable, LongWritable>.Context context)
			throws IOException, InterruptedException {
		sarahMetrics = new SarahMapReduceMetrics(context);
		// Get the target sample size from the configuration.  If not set defaults to 
		String sampleSizeString = sarahMetrics.getStringValue(SarahMetricNames.sarahTargetSampleFraction);
		if (sampleSizeString!=null) {
			targetSampleSize = Float.parseFloat(sampleSizeString);
		}
		nullSampleOutputKey = (sarahMetrics.getStringValue(SarahMetricNames.sarahSampleOutputKeyClass).equals("org.apache.hadoop.io.NullWritable"));
		// If a seed is set, then use it, otherwise let Java Random class assign it.
		String seedString = sarahMetrics.getStringValue(SarahMetricNames.sarahSampleSeed);
		multipleOutputs = new MultipleOutputs<KEYIN, VALUEIN>((TaskInputOutputContext<?, ?, KEYIN, VALUEIN>) context);
		if (seedString == null) {
			generator = new Random();
			
		} else {
			try {
				generator = new Random(Long.parseLong(seedString));
			} catch (NumberFormatException e) {
				generator = new Random(); 
			}
		}
				
	    Class<? extends Mapper<KEYIN, VALUEIN, WritableComparable, Writable>> mapperClass;
	    functions = sarahMetrics.getFunctions();
	    userMappers = new Mapper[functions.length];
		int i = 0;
		for (String function : functions) {
			String className = sarahMetrics.getStringValue(SarahMetricNames.stringForF(SarahMetricNames.sarahFunctionClass.name, function));
			try {
				mapperClass = (Class<? extends Mapper<KEYIN, VALUEIN, WritableComparable, Writable>>) Class.forName(className);
				userMappers[i++] = mapperClass.newInstance();
			} catch (Exception e) {
				System.err.println("Could not load "+className+". Check property value "+"sarah."+function+".class");
				e.printStackTrace();
			} 
		}
		super.setup(context);
	}

	@Override

	public void run(org.apache.hadoop.mapreduce.Mapper<KEYIN, VALUEIN, WritableComparable, LongWritable>.Context context)
			throws IOException, InterruptedException {
		setup(context);
		MultiFunctionMapper<KEYIN, VALUEIN>.Context mfmc = getMapContext(context);
		
		while (context.nextKeyValue()) {
			sarahMetrics.increment(SarahMetricNames.numberInputRecords,1);
			if (generator.nextDouble() < targetSampleSize) {
				sarahMetrics.increment(SarahMetricNames.numberSampleRecords,1);
				writeSample(context.getCurrentKey(),context.getCurrentValue());
				int i=0;
				for (String function : functions) {
					// set function name on context before calling mapper
					Mapper<KEYIN, VALUEIN, WritableComparable, Writable> mapper = userMappers[i++];
					// Would like to call map directly on the mapper but cannot do it
					// since it is declared to be protected.
					// The solution is to call run on each mapper for each record.
					// This works by passing MultiFunctionMapper.Context to the mapper
					// Mappers that override setup are going to get called many times.
					mfmc.setForMapper(function,context.getCurrentKey(),context.getCurrentValue());
					mapper.run(mfmc);
				}
			}
		}
		cleanup(context);
	}

	private void writeSample(
			KEYIN key, VALUEIN value) throws IOException, InterruptedException {
		if (nullSampleOutputKey) {
			multipleOutputs.write("sarahrandom", NullWritable.get(), value, "samples/random/part");
		} else {
			multipleOutputs.write("sarahrandom", key, value, "samples/random/part");
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		multipleOutputs.close();
		super.cleanup(context);
	}
	

}
