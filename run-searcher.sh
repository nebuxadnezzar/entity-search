#!/bin/bash

java -Xmx1100m -cp "./bin/entity-search.jar:$CLASSPATH" com.entity.indexing.Searcher $*