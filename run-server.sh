#!/bin/bash

java -Ddb.debug=t -Djetty.home=/tmp -jar bin/entity-search.jar server src/main/scripts/config/config.json