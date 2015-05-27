#!/usr/bin/env bash
git pull

rm -rf target
rm -f devenv

if [ -z "$JAVA_HOME" ]; then
  JAVA_HOME=/opt/taobao/java
fi

if [ -z "${M2_HOME}" ]; then
    M2_HOME=/opt/taobao/mvn
fi

export PATH=${M2_HOME}/bin:${JAVA_HOME}/bin:$PATH

mvn -Dmaven.test.skip=true clean package install assembly:assembly -U

ln -s target/alibaba-rocketmq-3.2.6/alibaba-rocketmq devenv
