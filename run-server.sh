#!/bin/bash

java -Ddb.debug=t -Djetty.home=./jetty.home -jar bin/entity-search.jar server src/main/scripts/config/config.json