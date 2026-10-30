#!/bin/bash
# CE-06: compile FailureSimulation.java against the local Hadoop classpath, build jar
set -e
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/opt/hadoop-3.4.2
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin

SRC_DIR=/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical/src
BUILD_DIR=$SRC_DIR/build
mkdir -p "$BUILD_DIR/classes"

CP=$("$HADOOP_HOME/bin/hadoop" classpath)
echo "Compiling against classpath: ${CP:0:120}..."
javac -cp "$CP" -d "$BUILD_DIR/classes" "$SRC_DIR/FailureSimulation.java"
echo "Compile OK. Creating jar..."
jar -cf "$BUILD_DIR/FailureSimulation.jar" -C "$BUILD_DIR/classes" .
ls -la "$BUILD_DIR/FailureSimulation.jar"
echo "JAR built: $BUILD_DIR/FailureSimulation.jar"
