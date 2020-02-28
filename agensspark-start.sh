#!/bin/bash

target_path=./target
jarfile=( "$target_path"/agenspop*.jar )
cfgfile=( ./*config.yml )
cfgname="${cfgfile%.*}"
echo $jarfile
[[ -e $jarfile ]] || {
  echo "ERROR: not exist agenspop jar file in backend/target/ \nTry build and start again.." >&2;
  exit 1;
  }

echo "Run target jar: $jarfile ($cfgname)"
java -jar $jarfile --spring.config.name=$cfgname
