#!/usr/bin/bash +x
echo $*

if [ $# -eq 0 ]; then
    echo "path to groovy script is missing"
    exit -1
fi

java -cp "./bin/entity-search.jar:$CLASSPATH" com.entity.tools.GroovyInterpreter $*