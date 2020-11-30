hdfs dfs -rm -r -f ./twitter_data_input
hdfs dfs -rm -r -f ./temp/input
hdfs dfs -rm -r -f ./temp/output
hdfs dfs -rm -r -f ./temp/first-mapreduce
hdfs dfs -rm -r -f ./temp/second-mapreduce
rm triplet.jar
rm ClosedTriplet*.class

hdfs dfs -mkdir -p ./temp/input
hdfs dfs -put ./data/twitter_data_input/tc1.net ./temp/input

hadoop com.sun.tools.javac.Main ClosedTripletCount.java
jar cf triplet.jar ClosedTripletCount*.class

hadoop jar triplet.jar ClosedTripletCount ./temp/input ./temp/output
hdfs dfs -cat ./temp/output/*
