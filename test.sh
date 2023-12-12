#!/bin/bash
for ((i=1;i<=50;i++));
do
    echo "ROUND $i";
    mkdir -p ./out;
    touch ./out/out-$i.txt;
    make project2b > ./out/out-$i.txt;
done