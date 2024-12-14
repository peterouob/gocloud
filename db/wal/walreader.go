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
	mu   sync.Mutex
	r    io.Reader
	seq  int
	i, j int
	n    int
	last bool
	buf  [blockSize]byte
	data []byte
}

func NewReader(r io.Reader) *Reader {
	reader := &Reader{
		r:    r,
		last: true,
	}
	return reader
}

var errSkip = errors.New("skipped")

func (r *Reader) corrupt(n int, reason string, skip bool) error {
	if !skip {
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
			} else if checksum != utils.NewCRC(r.buf[r.i-1:r.j]).Value() {
				r.i = r.n
				r.j = r.n
				return r.corrupt(unprocBlock, "check mismatch", false)
			}

			if first && chunkType != fullType && chunkType != firstType {
				chunkSize := (r.j - r.i) + headerSize
				r.i = r.j
				return r.corrupt(chunkSize, "orphan chunk", true)
			}

			//correct
			if first && (chunkType == fullType || chunkType == firstType) {
				r.data = r.buf[r.i:r.j]
			} else if chunkType == middleType {
				r.data = append(r.data, r.buf[r.i:r.j]...)
			} else if chunkType == lastType {
				r.data = append(r.data, r.buf[r.i:r.j]...)
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
