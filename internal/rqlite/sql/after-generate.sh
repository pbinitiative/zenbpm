#!/bin/bash

set -e

cp db.go.template db.go
sed -i "s/Foreign[[:space:]]\+interface{}[[:space:]]\+\`json:\"foreign\"\`//g" models.go
