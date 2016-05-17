#!/bin/bash

cd baleen

mkdir -p baleen/baleen-runner/models

for MODEL in location date money organization percentage person time
do
  if [ ! -e baleen-runner/models/en-ner-$MODEL.bin ]
  then
    wget -P baleen-runner/models/ http://opennlp.sourceforge.net/models-1.5/en-ner-$MODEL.bin 
  fi
done

if [ ! -e target/baleen-2.2.0-SNAPSHOT.jar ]
then
  mvn package -DskipTests=true
fi


nohup java -jar target/baleen-2.2.0-SNAPSHOT.jar sussex/confs/runner-miro.yaml  > ../baleen.log &

PID=$!

echo $PID > ../baleen.pid

printf "#!/bin/sh \n kill %s" "$PID" > ../stop.sh

chmod +x stop.sh

tail -f ../baleen.log

