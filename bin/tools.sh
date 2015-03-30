#!/bin/sh

#===========================================================================================
# Java Environment Setting
#===========================================================================================
error_exit ()
{
    echo "ERROR: $1 !!"
    exit 1
}

[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=$HOME/jdk/java
[ ! -e "$JAVA_HOME/bin/java" ] && JAVA_HOME=/opt/taobao/java
[ ! -e "$JAVA_HOME/bin/java" ] && error_exit "Please set the JAVA_HOME variable in your environment, We need java(x64)!"

export JAVA_HOME
export JAVA="$JAVA_HOME/bin/java"
export BASE_DIR=$(dirname $0)/..
export CLASSPATH=.:${BASE_DIR}/conf:${CLASSPATH}

#===========================================================================================
# JVM Configuration
#===========================================================================================
JAVA_OPT="${JAVA_OPT} -server -Xms1g -Xmx1g -Xmn256m -XX:PermSize=128m -XX:MaxPermSize=128m"
JAVA_OPT="${JAVA_OPT} -Djava.ext.dirs=${BASE_DIR}/lib:${JAVA_HOME}/jre/lib/ext"
JAVA_OPT="${JAVA_OPT} -Drocketmq.namesrv.domain=config.graphene.spellso.com"
JAVA_OPT="${JAVA_OPT} -Denable_ssl=true"
JAVA_OPT="${JAVA_OPT} -Duse_elastic_ip=true"
JAVA_OPT="${JAVA_OPT} -DRocketMQServerPassword=VVYZZ9NLVdy849XIy/tM3Q=="
JAVA_OPT="${JAVA_OPT} -DRocketMQClientPassword=VVYZZ9NLVdy849XIy/tM3Q=="
JAVA_OPT="${JAVA_OPT} -cp ${CLASSPATH}"

$JAVA ${JAVA_OPT} $@
