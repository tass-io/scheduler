#!/bin/sh
go build -buildmode=plugin ../pkg/initial/wrapper/plugin/plugin.go
zip plugin.zip ./plugin.so
mv ./plugin.zip ../user-code/plugin.zip
rm ./plugin.so