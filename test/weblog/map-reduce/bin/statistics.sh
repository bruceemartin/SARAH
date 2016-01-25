weblogData=weblog
echo "Compute SARAH statistics on $weblogData"
hadoop fs -rm -r $weblogData.sarah
sarah statistics -libjars $SARAH_TEST/weblog/map-reduce/lib/sarahtest.jar -conf $SARAH_TEST/weblog/map-reduce/bin/conf/statistics.xml $weblogData 
