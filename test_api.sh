#!/bin/bash

echo Pseudo-unit tests using gocqlmem and calling ProcessDataBatchMsg. See test_api.go comments for details. Take a while to run.

test(){
	local test_name=$1
    echo $test_name
	mkdir -p /var/tmp/capi_test/$test_name
	rm -fR /var/tmp/capi_test/$test_name/*
	go test -cover ./... -run $test_name -args -test.gocoverdir="/var/tmp/capi_test/$test_name"  | grep "/api"
}

test TestTableDoesNotExist
test TestOperationTimedOut
test TestDataSeriousError
test TestIdxSeriousError
test TestDataNotApplied
test TestIdxNotAppliedSamePresentFirstRetry
test TestIdxNotAppliedSamePresentSecondRetry
test TestIdxNotAppliedDiffPresent

mkdir -p /var/tmp/capi_test/test_api_merged
rm -fR /var/tmp/capi_test/test_api_merged/*
go tool covdata merge -i=/var/tmp/capi_test/TestTableDoesNotExist,/var/tmp/capi_test/TestOperationTimedOut,/var/tmp/capi_test/TestIdxSeriousError,/var/tmp/capi_test/TestDataNotApplied,/var/tmp/capi_test/TestIdxNotAppliedSamePresentFirstRetry,/var/tmp/capi_test/TestIdxNotAppliedSamePresentSecondRetry,/var/tmp/capi_test/TestIdxNotAppliedDiffPresent -o=/var/tmp/capi_test/test_api_merged
go tool covdata textfmt -i=/var/tmp/capi_test/test_api_merged -o=/var/tmp/capi_test/test_api.out
go tool cover -html=/var/tmp/capi_test/test_api.out -o=/var/tmp/test_api.html

echo Coverage report: /var/tmp/test_api.html