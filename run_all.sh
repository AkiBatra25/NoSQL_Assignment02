#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=~/hadoop-3.3.6
export PATH=$PATH:$HADOOP_HOME/bin
cd /mnt/c/Users/Ideapad/NoMoreOneDrive/Documents/College_this_sem/NoSQL/NoSQL_Assign2/NoSQL_Assignment02
export HADOOP_CLASSPATH=$(hadoop classpath)

echo "Compiling..."
rm -rf cooccurrence.jar classes/*
mkdir -p classes
javac -source 1.8 -target 1.8 -classpath "$HADOOP_CLASSPATH" -d classes StripesCooccurrence.java PairsCooccurrenceInMapper.java StripesCooccurrenceInMapper.java PairsCooccurrence.java PairsCooccurrenceCombiner.java StripesCooccurrenceCombiner.java
jar -cvf cooccurrence.jar -C classes/ .

INPUT_DIR="Wikipedia-50-ARTICLES"
CLASSES=( "PairsCooccurrence" "PairsCooccurrenceCombiner" "PairsCooccurrenceInMapper" "StripesCooccurrence" "StripesCooccurrenceCombiner" "StripesCooccurrenceInMapper" )

echo "Execution Times Log" > run_times_log.txt

for d in 1 2 3 4; do
    for CLASS_NAME in "${CLASSES[@]}"; do
        OUTPUT_DIR="out_${CLASS_NAME}_d${d}"
        
        echo "======================================"
        echo "Running $CLASS_NAME for distance $d"
        echo "======================================"
        
        rm -rf "$OUTPUT_DIR"
        
        START_TIME=$SECONDS
        hadoop jar cooccurrence.jar $CLASS_NAME $INPUT_DIR $OUTPUT_DIR $d
        END_TIME=$SECONDS
        RUNTIME=$((END_TIME - START_TIME))
        
        echo "$CLASS_NAME (d=$d) took $RUNTIME seconds." | tee -a run_times_log.txt
        
        TOP_OUT_FILE="top20_${CLASS_NAME}_d${d}.txt"
        
        if [[ "$CLASS_NAME" == *"Pairs"* ]]; then
            cat $OUTPUT_DIR/part-r-00000 | sort -nr -k2 | head -20 > "$TOP_OUT_FILE"
        else
            cat $OUTPUT_DIR/part-r-00000 | head -20 > "$TOP_OUT_FILE"
        fi
        
        echo "======================================"
        echo "Top 20 stored in $TOP_OUT_FILE:"
        cat "$TOP_OUT_FILE"
        echo "======================================"
        
        read -p "Take screenshot of the output. Press ENTER to continue to the next configuration..."
    done
done

echo "All runs completed. See run_times_log.txt for execution times."