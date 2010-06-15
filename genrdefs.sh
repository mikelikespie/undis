#!/usr/bin/env bash


REDIS_DIR=$1
REDIS_C=$REDIS_DIR/redis.c


cat <<GOFILE
package rdefs

const (
GOFILE

awk '/#define.*REDIS_CMD/ {print "	"$2" = "$3}' $REDIS_C

cat <<GOFILE
)
var cmds = map[string]int {
GOFILE

awk -F'[,{]' '/    \{.*REDIS/ {print "	"$2": "$5","}' $REDIS_C

echo "}"
