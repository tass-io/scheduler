#!/bin/sh
go build ../pkg/initial/wrapper/main.go
go build -buildmode=plugin ../pkg/initial/wrapper/plugin/plugin.go
zip plugin-golang-wrapper.zip ./main ./plugin.so
mv ./plugin-golang-wrapper.zip ../user-code/plugin-golang-wrapper.zip
rm ./main
rm ./plugin.so