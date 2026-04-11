#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=~/hadoop-3.3.6
export PATH=$PATH:$HADOOP_HOME/bin
cd /mnt/c/Users/Ideapad/NoMoreOneDrive/Documents/College_this_sem/NoSQL/NoSQL_Assign2/NoSQL_Assignment02
export HADOOP_CLASSPATH=$(hadoop classpath)
rm -rf cooccurrence.jar classes/*
javac -source 1.8 -target 1.8 -classpath "$HADOOP_CLASSPATH" -d classes StripesCooccurrence.java PairsCooccurrenceInMapper.java StripesCooccurrenceInMapper.java PairsCooccurrence.java PairsCooccurrenceCombiner.java StripesCooccurrenceCombiner.java
jar -cvf cooccurrence.jar -C classes/ .
