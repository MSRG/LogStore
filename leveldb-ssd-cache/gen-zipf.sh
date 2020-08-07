#!/bin/bash

YCSB_HOME=$(cd ../YCSB; pwd)
YCSB_VERSION="0.1.4"

if [ ! -d $YCSB_HOME ]; then
    echo "YCSB home \"$YCSB_HOME\" does not exist."
    exit 1
fi
echo "YCSB home exists at \"$YCSB_HOME\""

YCSB_JAR="$YCSB_HOME/core/target/core-$YCSB_VERSION.jar"
if [ ! -f $YCSB_JAR ]; then
    echo "YCSB jar \"$YCSB_JAR\" does not exist. Will try to build manually."
    YCSB_POM="$YCSB_HOME/pom.xml"
    if [ ! -f $YCSB_POM ]; then
        echo "No POM file, can't build."
        exit 1
    fi
    mvn -f clean package
fi

echo "YCSB Jar file exists at \"$YCSB_JAR\""

if [ -f $4 ]; then
    echo "WARN: Output file \"$4\" already exists ..."
    exit 1
fi

echo "Running Zipf Generator\n# Keys: $1, # Hits: $2, Skew: $3, Output to $4"
java -cp $YCSB_JAR com.yahoo.ycsb.generator.ZipfianGenerator $1 $2 $3 > $4 2>&1
exit
