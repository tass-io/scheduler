#!/bin/bash

cd $(dirname $0)

docker build -t registry.cn-shanghai.aliyuncs.com/tassio/scheduler:v$1 . && docker push registry.cn-shanghai.aliyuncs.com/tassio/scheduler:v$1
