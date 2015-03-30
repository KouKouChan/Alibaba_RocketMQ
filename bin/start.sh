#!/bin/bash

function init() {
   source `pwd`/env.sh
}

function name_server() {
   nohup sh mqnamesrv &
}

function broker() {
  if [ $1 == "master" ]; then
#     nohup sh mqbroker -n lizhanhui:9876\;holly:9876 -c ../conf/2m-2s-sync/broker-a.properties &
     if [ $# == 1 ] || [ $2 == "sync" ]; then
        nohup sh mqbroker -c ../conf/2m-2s-sync/broker-a.properties &
     elif [ $2 == "async" ]; then
        nohup sh mqbroker -c ../conf/2m-2s-async/broker-a.properties &
     fi
  elif [ $1 == "slave" ]; then
#     nohup sh mqbroker -n lizhanhui:9876\;holly:9876 -c ../conf/2m-2s-sync/broker-a-s.properties &
     if [ $# == 1 ] || [ $2 == "sync" ]; then
        nohup sh mqbroker -c ../conf/2m-2s-sync/broker-a-s.properties &
     elif [ $2 == "async" ]; then
        nohup sh mqbroker -c ../conf/2m-2s-async/broker-a-s.properties &
     fi
  else
     echo "Please enter role, be it 'master' or 'slave'";
  fi
}



function main() {
   init;

   case $1 in
      nameserver)
         name_server;
      ;;
      broker)
         broker $2 $3;
      ;;
      *)
      exit 1;
      ;;
  esac
}

main $@

