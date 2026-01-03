package ormx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestStruct struct {
}

func (*TestStruct) Table() string {
	return "my_struct"
}

func TestTableName(t *testing.T) {
	assert.Equal(t, "xxxx", TableName(TestStruct{}, &Option{table: "xxxx"}))
	assert.Equal(t, "my_struct", TableName(TestStruct{}, nil))
	assert.Equal(t, "my_struct", TableName(&TestStruct{}, nil))
	assert.Equal(t, "my_struct", TableName([]TestStruct{}, nil))
	assert.Equal(t, "my_struct", TableName([]*TestStruct{}, nil))
	assert.Equal(t, "my_struct", TableName(&[]*TestStruct{}, nil))
	assert.Equal(t, "my_struct", TableName(&[]TestStruct{}, nil))
}
