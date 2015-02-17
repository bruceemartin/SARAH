echo "Random sample of intermediate data of word length of shakespeare"
hadoop fs -rm -r shakespeare.isample
hadoop jar ../../../lib/sarah.jar sarah.sample.IntermediateSample -libjars ../../../lib/sarahtest.jar -conf conf/intermediateSample.xml shakespeare shakespeare.isample
