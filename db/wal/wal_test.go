package wal

import (
	"bytes"
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
