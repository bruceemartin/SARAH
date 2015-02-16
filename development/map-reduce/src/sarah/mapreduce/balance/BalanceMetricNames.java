package sarah.mapreduce.balance;

import sarah.mapreduce.util.SarahMetric;

public class BalanceMetricNames {
	// metrics generated by the balance tool
	public static SarahMetric recommendedNumberReducersForF = 
			new SarahMetric("sarah.%func.number.reducers","undescribed");  
	public static SarahMetric[] balanceMetrics = {recommendedNumberReducersForF};
}