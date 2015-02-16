echo "Compute SARAH statistics on weblog"
hadoop fs -rm -r weblog.sarah
sarah statistics -libjars ../../../lib/sarahtest.jar -conf conf/statistics.xml weblog 
