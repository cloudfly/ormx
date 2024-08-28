package test

import (
	"bytes"
	"path"
	"reflect"
	"runtime"
	"testing"
)

func NoError(t *testing.T, err error) {
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
}

func Equal(t *testing.T, expected, actual any) {
	_, file, line, _ := runtime.Caller(1)
	if !equal(expected, actual) {
		t.Logf("[%s:%d] %v not equal expected value %v", path.Base(file), line, actual, expected)
		t.FailNow()
	}
}

func equal(expected, actual any) bool {
	if expected == nil || actual == nil {
		return expected == actual
	}

	exp, ok := expected.([]byte)
	if !ok {
		return reflect.DeepEqual(expected, actual)
	}

	act, ok := actual.([]byte)
	if !ok {
		return false
	}
	if exp == nil || act == nil {
		return exp == nil && act == nil
	}
	return bytes.Equal(exp, act)
}
