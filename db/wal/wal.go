package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/peterouob/gocloud/db/utils"
	"io"
	"sync"
)

const (
	fullType = iota
	firstType
	middleType
	lastType
)

type flusher interface {
	Flush() error
}

type Dropper interface {
	Drop(err error)
}

const (
	blockSize  = 32 * 1024
	headerSize = 7
)

type ErrCorrupted struct {
	Size   int
	Reason string
}

func (e *ErrCorrupted) Error() string {
	return fmt.Sprintf("wal corrupted: %s (%d bytes)", e.Reason, e.Size)
}

type Reader struct {
	mu       sync.Mutex
	r        io.Reader
	dropper  Dropper
	strict   bool
	checksum bool
	seq      int
	i, j     int
	n        int
	last     bool
	buf      [blockSize]byte
	State    []interface{}
}

func NewReader(r io.Reader, dropper Dropper, strict, checksum bool) *Reader {
	reader := &Reader{
		r:        r,
		dropper:  dropper,
		strict:   strict,
		checksum: checksum,
		last:     true,
	}
	reader.State = append(reader.State, reader)
	return reader
}

var errSkip = errors.New("skipped")

func (r *Reader) corrupt(n int, reason string, skip bool) error {
	if r.dropper != nil {
		r.dropper.Drop(&ErrCorrupted{n, reason})
	}
	if r.strict && !skip {
		return fmt.Errorf("%v", &ErrCorrupted{n, reason})
	}
	return nil
}

func (r *Reader) nextChunk(first bool) error {
	for {
		if r.j+headerSize <= r.n {
			// definition
			checksum := binary.LittleEndian.Uint32(r.buf[r.j : r.j+4])
			length := binary.LittleEndian.Uint16(r.buf[r.j+4 : r.j+6])
			chunkType := r.buf[r.j+6]
			unprocBlock := r.n - r.j
			if checksum == 0 && length == 0 && chunkType == 0 {
				r.i = r.n
				r.j = r.n
				return r.corrupt(unprocBlock, "zero header", false)
			}
			if chunkType < fullType || chunkType > lastType {
				r.i = r.n
				r.j = r.n
				return r.corrupt(unprocBlock, fmt.Sprintf("invalid chunk type %#x", chunkType), false)
			}
			r.i = r.j + headerSize
			r.j = r.j + headerSize + int(length)
			if r.j > r.n {
				r.i = r.n
				r.j = r.n
				return r.corrupt(unprocBlock, "chunk overflow", false)
			} else if r.checksum && checksum != utils.NewCRC(r.buf[r.i-1:r.j]).Value() {
				r.i = r.n
				r.j = r.n
				return r.corrupt(unprocBlock, "check mismatch", false)
			}
			if first && chunkType != fullType && chunkType != firstType {
				chunkSize := (r.j - r.i) + headerSize
				r.i = r.j
				return r.corrupt(chunkSize, "orphan chunk", true)
			}
			r.last = (chunkType == fullType) || (chunkType == lastType)
			return nil
		}
		// last
		if r.n < blockSize && r.n > 0 {
			if !first {
				return r.corrupt(0, "missing chunk part", false)
			}
			return io.EOF
		}

		n, err := io.ReadFull(r.r, r.buf[:])
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			return err
		}
		if n == 0 {
			if !first {
				return r.corrupt(0, "missing chunk part", false)
			}
			return io.EOF
		}
		r.i = 0
		r.j = 0
		r.n = n
	}
}

func (r *Reader) Next() (io.Reader, error) {
	r.seq++
	r.i = r.j
	for {
		if err := r.nextChunk(true); err == nil {
			break
		} else if !errors.Is(err, errSkip) {
			return nil, err
		}
	}
	return &singleReader{r, r.seq, nil}, nil
}

type singleReader struct {
	r   *Reader
	seq int
	err error
}

func (s *singleReader) Read(p []byte) (int, error) {
	r := s.r
	if r.seq != s.seq {
		return 0, errors.New("stale reader")
	}
	if s.err != nil {
		return 0, s.err
	}
	for r.i == r.j {
		if r.last {
			return 0, io.EOF
		}
		s.err = r.nextChunk(false)
		if s.err != nil {
			if errors.Is(s.err, errSkip) {
				s.err = io.ErrUnexpectedEOF
			}
			return 0, s.err
		}
	}
	n := copy(p, r.buf[r.i:r.j])
	r.i += n
	return n, nil
}

func (s *singleReader) ReadByte() (byte, error) {
	r := s.r
	if r.seq != s.seq {
		return 0, errors.New("stale reader")
	}
	if s.err != nil {
		return 0, s.err
	}
	for r.i == r.j {
		if r.last {
			return 0, io.EOF
		}
		s.err = r.nextChunk(false)
		if s.err != nil {
			if errors.Is(s.err, errSkip) {
				s.err = io.ErrUnexpectedEOF
			}
			return 0, s.err
		}
	}
	c := r.buf[r.i]
	r.i++
	return c, nil
}

func (r *Reader) Reset(reader io.Reader, dropper Dropper, strict bool, checksum bool) {
	r.seq++
	r.r = reader
	r.dropper = dropper
	r.strict = strict
	r.checksum = checksum
	r.i = 0
	r.j = 0
	r.n = 0
	r.last = true
}

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
