#!/bin/bash

outdir='./bin'

mvn clean package

# this dumb ls is used here to emulate return of 0 and skip running mvn
# ls -1

if [[ $? -ne 0 ]]
then
    printf "buiding error. run maven separately as 'mvn clean package\n"
    exit 1
fi
#z=`find ./target/ -name "*with-dependencies.jar" -print`
z=`find ./target/ -name "*SNAPSHOT.jar" -print`

if [[ -z $z ]]
then
    echo "entity-search jar was not built"
else
    if [[ ! -d $outdir ]]
    then
        echo "dir doesn't exist"
        mkdir $outdir
    fi
    #mv $(echo $z | sed "s/-[0-9].[0-9]-SNAPSHOT-jar-with-dependencies//i") ${outdir}
    mv ${z} "${outdir}/entity-search.jar"
fi
