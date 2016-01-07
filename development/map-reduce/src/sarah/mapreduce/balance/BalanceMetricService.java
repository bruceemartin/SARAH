package sarah.mapreduce.balance;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import sarah.mapreduce.metrics.MapReduceMetricService;

public class BalanceMetricService extends MapReduceMetricService {

	public BalanceMetricService(Job job,String pathName) throws IOException {
		super(job,pathName);
		new BalanceSarahMetrics(job.getConfiguration());
	}

	public BalanceMetricService(TaskInputOutputContext<?, ?, ?, ?> ctx) throws IOException {
		super(ctx);
		new BalanceSarahMetrics(ctx.getConfiguration());
	}

}
