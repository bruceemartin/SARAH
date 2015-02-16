echo "Evening test of weblog with $1 reducers"
hadoop fs -rm -r weblog.evening.$1
hadoop jar ../../../lib/sarahtest.jar sarah.sample.test.EveningTest $1 weblog weblog.evening.$1
hadoop fs -getmerge weblog.evening.$1 output/weblog.evening.$1
