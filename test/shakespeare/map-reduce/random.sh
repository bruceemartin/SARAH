echo "Random sample of shakespeare"
hadoop fs -rm -r shakespeare.sample
hadoop jar ../../../lib/sarah.jar sarah.sample.RandomSample -conf conf/randomSample.xml shakespeare shakespeare.sample
