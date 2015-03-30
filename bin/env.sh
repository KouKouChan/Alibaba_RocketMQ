#!/bin/sh
CLASSPATH="."
libJars=`ls ../lib/*.jar`
for libjar in ${libJars}
do
    CLASSPATH="${CLASSPATH}:${libjar}"
done
echo $CLASSPATH
ROCKETMQ_ENABLE_SSL=true
export CLASSPATH ROCKETMQ_ENABLE_SSL
