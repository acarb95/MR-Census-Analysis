#!/bin/bash

for i in `cat ~/455HadoopConf/slaves`
do
    gnome-terminal -x bash -c "ssh -t ${i} 'rm -rf /tmp/hadoop-acarbona/; rm -rf /tmp/acarbona/; rm -rf /s/${i}/a/tmp/acarbona/'" &
done

gnome-terminal -x bash -c "ssh -t alexandra 'rm -rf /tmp/hadoop-acarbona/; rm -rf /tmp/acarbona/; rm -rf /s/alexandra/a/tmp/acarbona/'" &
