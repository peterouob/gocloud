package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/peterouob/gocloud/db/utils"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
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
	fd          *os.File
	count       int
	fdSize      int
}

const maxFdSize = 100

func NewWriter(w io.Writer) *Writer {
	f, _ := w.(flusher)
	writer := &Writer{
		w: w,
		f: f,
	}
	if err := writer.rotationWALFile(); err != nil {
		panic(errors.New("error in rotation wal file: " + err.Error()))
	}
	return writer
}

func (w *Writer) rotationWALFile() error {

	log.Println("create file...")
	err := os.MkdirAll("./log/", 0755)
	if !errors.Is(err, os.ErrExist) && err != nil {
		panic(errors.New("error in call os.MkdirAll: " + err.Error()))
	}
	w.count++
	filename := fmt.Sprintf("wal_%d.log", w.count)
	path := filepath.Join("./log/", filename)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return fmt.Errorf("failed to create WAL file: %v", err)
	}

	w.fd = file
	w.w = io.MultiWriter(w.w, file)
	return nil
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

func (w *Writer) writeBlock() error {

	if w.mu.TryLock() {
		defer w.mu.Unlock()
	}

	if _, err := w.w.Write(w.buf[w.written:w.j]); err != nil {
		return fmt.Errorf("failed to write to writer: %v", err)
	}

write:
	if w.fdSize < 20 {
		if w.fd != nil {
			if _, err := w.fd.Write(w.buf[w.written:w.j]); err != nil {
				return fmt.Errorf("failed to write to file: %v", err)
			}
			w.fdSize += len(w.buf[w.written:w.j])
			log.Println(w.fdSize)
		}
	} else {
		err := w.rotationWALFile()
		if err != nil {
			return err
		}
		w.fdSize = int(math.Abs(float64(20 - int(w.fdSize))))
		goto write
	}

	w.i = 0
	w.j = headerSize
	w.written = 0
	w.blockNumber++

	return nil
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

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.seq++

	if w.j > headerSize {
		if err := w.writeBlock(); err != nil {
			return err
		}
	}

	if w.fd != nil {
		if err := w.fd.Sync(); err != nil {
			return fmt.Errorf("failed to sync file: %v", err)
		}

		if err := w.fd.Close(); err != nil {
			return fmt.Errorf("failed to close file: %v", err)
		}
		w.fd = nil
	}

	return nil
}

func (w *Writer) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.seq++
	w.writePending()
	if w.f != nil {
		err := w.f.Flush()
		if err != nil {
			panic(errors.New("error in call Flush: " + err.Error()))
		}
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
		err := w.writeBlock()
		if err != nil {
			panic(errors.New("error in call w.writeBlock: " + err.Error()))
		}
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
			err := w.writeBlock()
			if err != nil {
				return n0, errors.New("error in call w.writeBlock: " + err.Error())
			}
			w.first = false
		}
		n := copy(w.buf[w.j:], p)
		w.j += n
		p = p[n:]
	}
	return n0, nil
}
