package wal

import (
	"encoding/binary"
	"errors"
	"github.com/peterouob/gocloud/db/utils"
	"io"
	"sync"
)

type Writer struct {
	mu          sync.Mutex
	w           io.Writer
	f           flusher
	seq         int
	i, j        int
	written     int
	blockNumber int64
	first       bool
	pending     bool
	buf         [blockSize]byte
}

func NewWriter(w io.Writer) *Writer {
	f, _ := w.(flusher)
	return &Writer{
		w: w,
		f: f,
	}
}

func (w *Writer) fillHeader(last bool) {
	if w.i+headerSize > w.j || w.j > blockSize {
		panic(errors.New("error in fillHeader"))
	}
	if last {
		if w.first {
			w.buf[w.i+6] = fullType
		} else {
			w.buf[w.i+6] = lastType
		}
	} else {
		if w.first {
			w.buf[w.i+6] = firstType
		} else {
			w.buf[w.i+6] = middleType
		}
	}

	binary.LittleEndian.PutUint32(w.buf[w.i:w.i+4], utils.NewCRC(w.buf[w.i+6:w.j]).Value())
	binary.LittleEndian.PutUint16(w.buf[w.i+4:w.i+6], uint16(w.j-w.i-headerSize))
}

func (w *Writer) writeBlock() {
	if _, err := w.w.Write(w.buf[w.written:]); err != nil {
		panic(errors.New("error in call w.w.Write in writeBlock" + err.Error()))
	}

	w.i = 0
	w.j = headerSize
	w.written = 0
	w.blockNumber++
}

func (w *Writer) writePending() {
	if w.pending {
		w.fillHeader(true)
		w.pending = false
	}

	if _, err := w.w.Write(w.buf[w.written:w.j]); err != nil {
		panic(errors.New("error in call w.w.Write in writePending" + err.Error()))
	}
	w.written = w.j
}

func (w *Writer) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.seq++
	w.writePending()
}

func (w *Writer) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.seq++
	w.writePending()

	if w.f != nil {
		w.f.Flush()
	}
}
func (w *Writer) Reset(writer io.Writer) {
	w.seq++
	w.writePending()
	w.w = writer
	w.f, _ = writer.(flusher)
	w.i = 0
	w.j = 0
	w.written = 0
	w.blockNumber = 0
	w.first = false
	w.pending = false
}

func (w *Writer) Next() io.Writer {
	w.seq++
	if w.pending {
		w.fillHeader(true)
	}
	w.i = w.j
	w.j += headerSize
	if w.j > blockSize {
		for k := w.i; k < blockSize; k++ {
			w.buf[k] = 0
		}
		w.writeBlock()
	}
	w.first = true
	w.pending = true
	return singleWriter{w, w.seq}
}

func (w *Writer) Size() int64 {
	if w == nil {
		return 0
	}
	return w.blockNumber*blockSize + int64(w.j)
}

type singleWriter struct {
	w   *Writer
	seq int
}

func (s singleWriter) Write(p []byte) (int, error) {
	w := s.w
	if w.seq != s.seq {
		return 0, errors.New("error in singleWriter stale writer")
	}
	n0 := len(p)
	for len(p) > 0 {
		if w.j == blockSize {
			w.fillHeader(false)
			w.writeBlock()
			w.first = false
		}
		n := copy(w.buf[w.j:], p)
		w.j += n
		p = p[n:]
	}
	return n0, nil
}
