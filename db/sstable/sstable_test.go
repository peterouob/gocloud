package sstable

import (
	"github.com/peterouob/gocloud/db/config"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"path"
	"testing"
)

func TestSSTableWriteAndRead(t *testing.T) {
	conf := &config.Config{
		Dir:                 os.TempDir(),
		SstFooterSize:       40,
		SstRestartInterval:  16,
		SstDataBlockSize:    4096,
		SstBlockTrailerSize: 4,
	}

	filename := "test.sst"
	filepath := path.Join(conf.Dir, filename)

	os.Remove(filepath)

	testData := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value4"},
	}

	var fileSize int64
	var filterMap map[uint64][]byte
	var indices []*Index

	// Write test
	t.Run("Write SSTable", func(t *testing.T) {
		writer, err := NewSStWriter(filename, conf)
		assert.NoError(t, err)

		log.Println("Writing test data...")
		for _, data := range testData {
			writer.Append([]byte(data.key), []byte(data.value))
			log.Printf("Appended key: %s, value: %s", data.key, data.value)
		}

		fileSize, filterMap, indices, err = writer.Finish()
		assert.NoError(t, err)
		assert.Greater(t, fileSize, int64(0))
		assert.NotEmpty(t, filterMap)
		assert.NotEmpty(t, indices)

		log.Printf("File size: %d", fileSize)
		log.Printf("Number of filters: %d", len(filterMap))
		log.Printf("Number of indices: %d", len(indices))

		for offset, filter := range filterMap {
			log.Printf("Filter at offset %d, size: %d", offset, len(filter))
		}

		for _, idx := range indices {
			log.Printf("Index key: %s, offset: %d, size: %d",
				string(idx.Key), idx.PrevOffset, idx.PrevSize)
		}

		writer.Close()
	})
}
