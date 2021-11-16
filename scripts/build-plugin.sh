#!/bin/sh
go build -buildmode=plugin ../pkg/initial/wrapper/plugin/plugin.go
mv ./plugin.so ../user-code/plugin.so