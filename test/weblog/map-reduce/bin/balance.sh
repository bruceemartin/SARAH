weblogData=weblog
echo "Balance reducers using SARAH statistics on $weblogData"
hadoop fs -rm -r weblog.sarah/artifacts/balanced-reducers 
sarah balanced-reducers -libjars ../../../lib/sarahtest.jar -conf conf/balance.xml $weblogData
