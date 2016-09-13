#!/bin/bash

function name_server() {
   nohup sh mqnamesrv &
}

function broker() {
  if [ $1 == "master" ]; then
     if [ $# == 1 ] || [ $2 == "sync" ]; then
        if [ $# -lt 3 ] || [ $3 == "a" ]; then
            nohup sh mqbroker -c ../conf/2m-2s-sync/broker-a.properties &
        elif [ $3 == "b" ]; then
            nohup sh mqbroker -c ../conf/2m-2s-sync/broker-b.properties &
        fi
     elif [ $2 == "async" ]; then
        if [ $# -lt 3 ] || [ $3 == "a" ]; then
           nohup sh mqbroker -c ../conf/2m-2s-async/broker-a.properties &
        elif [ $3 == "b" ]; then
           nohup sh mqbroker -c ../conf/2m-2s-async/broker-b.properties &
        fi
     fi
  elif [ $1 == "slave" ]; then
     if [ $# == 1 ] || [ $2 == "sync" ]; then
        if [ $# -lt 3 ] || [ $3 == "a" ]; then
           nohup sh mqbroker -c ../conf/2m-2s-sync/broker-a-s.properties &
        elif [ $3 == "b" ]; then
           nohup sh mqbroker -c ../conf/2m-2s-sync/broker-b-s.properties &
        fi
     elif [ $2 == "async" ]; then
        if [ $# -lt 3 ] || [ $3 == "a" ]; then
           nohup sh mqbroker -c ../conf/2m-2s-async/broker-a-s.properties &
        elif [ $3 == "b" ]; then
           nohup sh mqbroker -c ../conf/2m-2s-async/broker-b-s.properties &
        fi
     fi
  else
     echo "Please enter role, be it 'master' or 'slave'";
  fi
}

function main() {
   case $1 in
      nameserver)
         name_server;
      ;;
      broker)
         if [ $# == 3 ]; then
            broker $2 $3;
         elif [ $# == 4 ]; then
            broker $2 $3 $4;
         fi
      ;;
      *)
      exit 1;
      ;;
  esac
}

main $@

