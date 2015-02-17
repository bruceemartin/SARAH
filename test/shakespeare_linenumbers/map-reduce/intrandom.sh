echo "Random sample of intermediate data of word length of shakespeare with line numbers"
hadoop fs -rm -r shakespeare_linenumbers.isample
hadoop jar ../../../lib/sarah.jar sarah.sample.IntermediateSample -libjars ../../../lib/sarahtest.jar -conf conf/intermediateSample.xml shakespeare_linenumbers shakespeare_linenumbers.isample
