package sstable

import (
	"testing"

	"github.com/peterouob/gocloud/db/config"
	"github.com/stretchr/testify/assert"
)

func testNewSStWriter(t *testing.T) *SsWriter {
	writer, err := NewSStWriter("1.sst", config.NewConfig("./sst"))
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	return writer
}

func TestWrite(t *testing.T) {
	w := testNewSStWriter(t)
	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	values := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}

	for i := 0; i < len(keys); i++ {
		w.Append(keys[i], values[i])
	}
	w.Finish()
}
