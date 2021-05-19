#!/bin/sh
go build ../pkg/initial/wrapper/main.go
zip default-golang-wrapper.zip ./main
mv ./default-golang-wrapper.zip ../user-code/default-golang-wrapper.zip
rm ./main