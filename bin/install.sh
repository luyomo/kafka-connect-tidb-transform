#!/bin/bash

TargetServer=$1

rsync target/TiDBTransform-1.0-SNAPSHOT.jar $TargetServer:/tmp/
ssh $TargetServer "sudo mv /tmp/TiDBTransform-1.0-SNAPSHOT.jar /usr/share/confluent-hub-components/"
ssh $TargetServer "sudo systemctl restart confluent-kafka-connect"
