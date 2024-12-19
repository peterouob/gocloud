package sstable

import (
	"bytes"
	"fmt"
	"github.com/peterouob/gocloud/db/config"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"path"
	"testing"
)

func TestSSTableWriteAndRead(t *testing.T) {
	// Setup
	conf := &config.Config{
		Dir:                 os.TempDir(),
		SstFooterSize:       40,
		SstRestartInterval:  16,
		SstDataBlockSize:    4096,
		SstBlockTrailerSize: 4,
	}

	filename := "test.sst"
	filepath := path.Join(conf.Dir, filename)

	// Ensure clean state
	os.Remove(filepath)

	// Test data
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

	// Read test
	t.Run("Read SSTable", func(t *testing.T) {
		reader, err := NewSStReader(filename, conf)
		assert.NoError(t, err)

		log.Println("Reading footer...")
		err = reader.ReadFooter()
		assert.NoError(t, err)

		log.Printf("Footer read - FilterOffset: %d, FilterSize: %d, IndexOffset: %d, IndexSize: %d",
			reader.FilterOffset, reader.FilterSize, reader.IndexOffset, reader.IndexSize)

		// Read and verify filter
		filter, err := reader.ReadFilter()
		assert.NoError(t, err)
		assert.NotEmpty(t, filter)
		log.Printf("Read %d filters", len(filter))

		// Read and verify index
		indices, err := reader.ReadIndex()
		assert.NoError(t, err)
		assert.NotEmpty(t, indices)
		log.Printf("Read %d indices", len(indices))

		// Verify data blocks
		log.Println("Reading data blocks...")
		for i, index := range indices {
			log.Printf("Reading block %d - offset: %d, size: %d", i, index.PrevOffset, index.PrevSize)

			block, err := reader.ReadBlock(index.PrevOffset, index.PrevSize)
			assert.NoError(t, err)
			assert.NotEmpty(t, block)

			data, restarts, err := DecodeBlock(block)
			assert.NotEmpty(t, data)
			assert.NotEmpty(t, restarts)
			assert.NoError(t, err)

			buf := bytes.NewBuffer(data)
			prevKey := make([]byte, 0)

			for {
				key, value, err := ReadRecord(prevKey, buf)
				if err != nil {
					break
				}

				log.Printf("Read record - key: %s, value: %s", string(key), string(value))

				// Verify against test data
				found := false
				for _, td := range testData {
					if bytes.Equal(key, []byte(td.key)) {
						assert.Equal(t, td.value, string(value))
						found = true
						break
					}
				}
				assert.True(t, found, fmt.Sprintf("Key %s not found in test data", string(key)))

				prevKey = key
			}
		}

		reader.Destroy()
	})

	// Cleanup
	t.Cleanup(func() {
		os.Remove(filepath)
	})
}

func TestBlockOperations(t *testing.T) {
	conf := &config.Config{
		SstRestartInterval:  16,
		SstBlockTrailerSize: 4,
	}

	t.Run("Block Write and Read", func(t *testing.T) {
		block := NewBlock(conf)

		// Test data
		testData := []struct {
			key   string
			value string
		}{
			{"key1", "value1"},
			{"key2", "value2"},
			{"key3", "value3"},
		}

		// Write to block
		for _, data := range testData {
			block.Append([]byte(data.key), []byte(data.value))
		}

		// Get compressed data
		compressed := block.compress()
		assert.NotEmpty(t, compressed)

		// Write to buffer
		buf := new(bytes.Buffer)
		n, err := block.FlushBlockTo(buf)
		assert.NoError(t, err)
		assert.Greater(t, n, uint64(0))

		// Read and decode
		data, restarts, err := DecodeBlock(buf.Bytes())
		assert.NotEmpty(t, data)
		assert.NotEmpty(t, restarts)
		assert.NoError(t, err)

		// Verify records
		readBuf := bytes.NewBuffer(data)
		prevKey := make([]byte, 0)

		for _, td := range testData {
			key, value, err := ReadRecord(prevKey, readBuf)
			assert.NoError(t, err)
			assert.Equal(t, td.key, string(key))
			assert.Equal(t, td.value, string(value))
			prevKey = key
		}
	})
}
