package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/peterouob/gocloud/db/utils"
	"github.com/stretchr/testify/assert"
	"io"
	"log"
	"testing"
)

func TestEmpty(t *testing.T) {
	buf := new(bytes.Buffer)
	r := NewReader(buf)
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("need=%v, got=%v", io.EOF, err)
	}
}

func createTestChunk(chunkType byte, data []byte, checksum bool) []byte {
	chunk := make([]byte, headerSize+len(data))

	chunk[6] = chunkType

	binary.LittleEndian.PutUint16(chunk[4:6], uint16(len(data)))

	copy(chunk[headerSize:], data)

	if checksum {
		checksumValue := utils.NewCRC(chunk[6:]).Value()
		binary.LittleEndian.PutUint32(chunk[0:4], checksumValue)
	}

	return chunk
}

func TestWriteReader(t *testing.T) {
	buf := new(bytes.Buffer)

	writer := NewWriter(buf)
	defer writer.Close()

	data := []byte("test")
	w := writer.Next()
	_, err := w.Write(data)
	assert.NoError(t, err)

	writer.Flush()

	log.Printf("File path: %s", writer.fd.Name())

	reader := NewReader(buf)
	chunk, err := reader.Next()
	assert.NoError(t, err)

	rdata, err := io.ReadAll(chunk)
	assert.NoError(t, err)
	assert.Equal(t, data, rdata)
}

func TestManyWriteReader(t *testing.T) {
	buf := new(bytes.Buffer)

	writer := NewWriter(buf)
	defer writer.Close()

	data := []byte("test")
	for i := 0; i < 10000; i++ {
		w := writer.Next()
		_, err := w.Write(data)
		assert.NoError(t, err)
		writer.Flush()
	}

	log.Printf("File path: %s", writer.fd.Name())

	reader := NewReader(buf)
	chunk, err := reader.Next()
	assert.NoError(t, err)

	rdata, err := io.ReadAll(chunk)
	assert.NoError(t, err)
	assert.Equal(t, data, rdata)
}

func TestWriterBlockBoundary(t *testing.T) {
	buf := new(bytes.Buffer)
	writer := NewWriter(buf)

	largeData := bytes.Repeat([]byte("a"), blockSize*2)

	singleWriter := writer.Next()
	n, err := singleWriter.Write(largeData)
	assert.NoError(t, err)
	assert.Equal(t, len(largeData), n)

	writer.Close()

	reader := NewReader(buf)
	chunk, err := reader.Next()
	assert.NoError(t, err)

	readData, err := io.ReadAll(chunk)
	assert.NoError(t, err)
	assert.Equal(t, largeData, readData)
}

func TestReaderErrorHandling(t *testing.T) {
	testCases := []struct {
		name           string
		chunks         [][]byte
		expectError    bool
		expectedErrors int
	}{{
		name: "Orphan Chunk",
		chunks: [][]byte{
			createTestChunk(middleType, []byte("test"), true),
		},
		expectError:    false,
		expectedErrors: 1,
	},
		{
			name: "Zero Header",
			chunks: [][]byte{
				make([]byte, headerSize),
			},
			expectError:    true,
			expectedErrors: 1,
		},
		{
			name: "Invalid Chunk Type",
			chunks: [][]byte{
				func() []byte {
					chunk := createTestChunk(firstType, []byte("test"), true)
					chunk[6] = 0xFF // Invalid chunk type
					return chunk
				}(),
			},
			expectError:    true,
			expectedErrors: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := new(bytes.Buffer)

			// Write test chunks
			for _, chunk := range tc.chunks {
				buf.Write(chunk)
			}

			reader := NewReader(buf)
			_, err := reader.Next()

			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestWriterReset(t *testing.T) {
	buf1 := new(bytes.Buffer)
	buf2 := new(bytes.Buffer)

	writer := NewWriter(buf1)

	singleWriter := writer.Next()
	_, err := singleWriter.Write([]byte("first"))
	assert.NoError(t, err)

	writer.Reset(buf2)

	singleWriter = writer.Next()
	_, err = singleWriter.Write([]byte("second"))
	assert.NoError(t, err)

	writer.Close()

	reader := NewReader(buf2)
	chunk, err := reader.Next()
	assert.NoError(t, err)

	data, err := io.ReadAll(chunk)
	assert.NoError(t, err)
	assert.Equal(t, "second", string(data))
}

func TestWriterSize(t *testing.T) {
	buf := new(bytes.Buffer)
	writer := NewWriter(buf)

	assert.Equal(t, int64(0), writer.Size())

	singleWriter := writer.Next()
	_, err := singleWriter.Write([]byte("test"))
	assert.NoError(t, err)

	writer.Close()

	// PrevSize should be more than 0 after writing
	assert.Greater(t, writer.Size(), int64(0))
}

func FuzzWriterReader(f *testing.F) {
	f.Add([]byte("test data"))
	f.Fuzz(func(t *testing.T, data []byte) {
		buf := new(bytes.Buffer)
		writer := NewWriter(buf)

		singleWriter := writer.Next()
		_, err := singleWriter.Write(data)
		assert.NoError(t, err)

		writer.Close()

		reader := NewReader(buf)
		chunk, err := reader.Next()
		assert.NoError(t, err)

		readData, err := io.ReadAll(chunk)
		assert.NoError(t, err)
		assert.Equal(t, data, readData)
	})
}

func debugPrintBuffer(buf *bytes.Buffer) {
	data := buf.Bytes()
	fmt.Println("Buffer contents:")
	for i := 0; i < len(data); i += headerSize {
		end := min(i+headerSize, len(data))
		chunk := data[i:end]
		fmt.Printf("Chunk %d: %v\n", i/headerSize, chunk)
		if len(chunk) >= 7 {
			fmt.Printf("  Checksum: %x\n", chunk[0:4])
			fmt.Printf("  Length: %d\n", binary.LittleEndian.Uint16(chunk[4:6]))
			fmt.Printf("  Type: %x\n", chunk[6])
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func TestWriterBasicUsage(t *testing.T) {
	buf := new(bytes.Buffer)
	writer := NewWriter(buf)

	singleWriter1 := writer.Next()
	n, err := singleWriter1.Write([]byte("hello"))
	assert.NoError(t, err, "First write failed")
	assert.Equal(t, 5, n, "Incorrect number of bytes written")

	singleWriter2 := writer.Next()
	n, err = singleWriter2.Write([]byte("world"))
	assert.NoError(t, err, "Second write failed")
	assert.Equal(t, 5, n, "Incorrect number of bytes written")

	writer.Close()

	debugPrintBuffer(buf)

	reader := NewReader(buf)

	chunk1, err := reader.Next()
	assert.NoError(t, err, "Failed to get first chunk")

	data1, err := io.ReadAll(chunk1)
	assert.NoError(t, err, "Failed to read first chunk")
	assert.Equal(t, "hello", string(data1), "First chunk data mismatch")

	chunk2, err := reader.Next()
	assert.NoError(t, err, "Failed to get second chunk")

	data2, err := io.ReadAll(chunk2)
	assert.NoError(t, err, "Failed to read second chunk")
	assert.Equal(t, "world", string(data2), "Second chunk data mismatch")
}

func TestChunkCreation(t *testing.T) {
	buf := new(bytes.Buffer)
	writer := NewWriter(buf)

	singleWriter := writer.Next()
	data := []byte("test")
	n, err := singleWriter.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n)

	writer.Close()

	debugPrintBuffer(buf)

	bufBytes := buf.Bytes()

	assert.GreaterOrEqual(t, len(bufBytes), headerSize, "Buffer too small")

	assert.True(t, bufBytes[6] == fullType || bufBytes[6] == firstType,
		fmt.Sprintf("Invalid chunk type: %x", bufBytes[6]))

	length := binary.LittleEndian.Uint16(bufBytes[4:6])
	assert.Equal(t, uint16(len(data)), length, "Incorrect length in header")
}
