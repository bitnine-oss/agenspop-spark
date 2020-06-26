#!/bin/bash

jarfile=`ls target/agenspop*.jar`
#echo $jarfile
cfgfile=`ls *.yml`
cfgname="${cfgfile%.yml}"
#echo $cfgfile : $cfgname

if [ ! -f $jarfile ]; then
  echo "ERROR: not exist agenspop jar file in ./target/ \nTry build and start again.." >&2;
  exit 1;
fi

echo "Run target jar: "${jarfile}"("${cfgname}")"
java -jar $jarfile --spring.config.name=$cfgname
