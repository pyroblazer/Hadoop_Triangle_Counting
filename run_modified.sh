hdfs dfs -rm -r -f ./output
hdfs dfs -rm -r -f ./input
hdfs dfs -rm -r -f ./temp/first-mapreduce
hdfs dfs -rm -r -f ./temp/second-mapreduce
rm triplet.jar
rm ClosedTriplet*.class
hdfs dfs -mkdir -p ./input
hdfs dfs -put ./data/tc6.net ./input
hadoop com.sun.tools.javac.Main ClosedTripletCount.java
jar cf triplet.jar ClosedTripletCount*.class
hadoop jar triplet.jar ClosedTripletCount ./input ./output
hdfs dfs -cat ./output/*
hdfs dfs -cat ./temp/first-mapreduce/*
