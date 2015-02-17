echo "Random sample of shakespeare with line numbers"
hadoop fs -rm -r shakespeare_linenumbers.sample
hadoop jar ../../../lib/sarah.jar sarah.sample.RandomSample -conf conf/randomSample.xml shakespeare_linenumbers shakespeare_linenumbers.sample
