#!/bin/bash
for ((i=1;i<=20;i++));
do
    echo "ROUND $i";
    mkdir -p ./out;
    touch ./out/out2c-$i.txt;
    make project2c > ./out/out2c-$i.txt;
done