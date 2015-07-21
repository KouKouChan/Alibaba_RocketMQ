#!/bin/sh
git pull

rm -rf target
rm -f devenv

if [ -z "$JAVA_HOME" ]; then
    JAVA_HOME=/opt/taobao/java
fi
echo "JAVA_HOME=${JAVA_HOME}"

if [ -z "$M2_HOME" ]; then
    M2_HOME=/opt/taobao/mvn
fi
echo "M2_HOME=${M2_HOME}"

export PATH=${M2_HOME}/bin:$JAVA_HOME/bin:$PATH

set MAVEN_OPTS=-Xms512M -Xmx512M -XX:MaxPermSize=256M -XX:PermSize=256M
mvn -Dmaven.test.skip=true clean package install assembly:assembly -U

ln -s target/alibaba-rocketmq-3.2.2.R2-FB/alibaba-rocketmq devenv
cp ${JAVA_HOME}/jre/lib/ext/sunjce_provider.jar devenv/lib/
