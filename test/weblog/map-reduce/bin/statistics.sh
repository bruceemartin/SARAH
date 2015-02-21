echo "Compute SARAH statistics on weblog"
hadoop fs -rm -r weblog.sarah
sarah statistics -libjars $SARAH_TEST/weblog/map-reduce/lib/sarahtest.jar -conf $SARAH_TEST/weblog/map-reduce/bin/conf/statistics.xml weblog 
