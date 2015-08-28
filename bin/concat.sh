#!/bin/bash

FILE=/s/bach/k/under/acarbona/cs455/HW3_Census/src/output

echo "Concatenating files"

for i in `seq 0 9`
do
    if [ "$i" -lt "10" ] ; then
        cat $FILE/part-r-0000$i >> $FILE/concatoutput.txt
    else
        cat $FILE/part-r-000$i >> $FILE/concatoutput.txt
    fi
done

echo "All done!"
