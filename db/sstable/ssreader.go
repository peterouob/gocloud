package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/golang/snappy"
	"github.com/peterouob/gocloud/db/config"
	"github.com/peterouob/gocloud/db/utils"
	"io"
	"os"
	"path"
	"sync"
)

type SStReader struct {
	mu           sync.Mutex
	conf         *config.Config
	fd           *os.File
	reader       *bufio.Reader
	FilterOffset int64
	FilterSize   int64
	IndexOffset  int64
	IndexSize    int64
	compress     []byte
}

func NewSStReader(file string, conf *config.Config) (*SStReader, error) {
	fd, err := os.OpenFile(path.Join(conf.Dir, file), os.O_RDONLY, 0644)
	if err != nil {
		return nil, errors.New("error in open file : " + err.Error())
	}

	return &SStReader{
		conf:   conf,
		fd:     fd,
		reader: bufio.NewReader(fd),
	}, nil
}

func (r *SStReader) ReadFooter() error {
	_, err := r.fd.Seek(-int64(r.conf.SstFooterSize), io.SeekEnd)
	if err != nil {
		return errors.New("error in r.fd.Seek :" + err.Error())
	}
	filterOffset, err := binary.ReadUvarint(r.reader)
	if err != nil {
		return errors.New("error in binary.ReadUvarint get filterOffset: " + err.Error())
	}
	filterSize, err := binary.ReadUvarint(r.reader)
	if err != nil {
		return errors.New("error in binary.ReadUvarint get filterSize: " + err.Error())
	}

	indexOffset, err := binary.ReadUvarint(r.reader)
	if err != nil {
		return errors.New("error in binary.ReadUvarint get indexOffset: " + err.Error())
	}

	indexSize, err := binary.ReadUvarint(r.reader)
	if err != nil {
		return errors.New("error in binary.ReadUvarint get indexSize: " + err.Error())
	}

	if filterOffset == 0 || filterSize == 0 || indexOffset == 0 || indexSize == 0 {
		return errors.New("sst footer data error")
	}

	r.FilterOffset = int64(filterOffset)
	r.FilterSize = int64(filterSize)
	r.IndexOffset = int64(indexOffset)
	r.IndexSize = int64(indexSize)

	return nil
}

func (r *SStReader) readBlock(offset, size int64) ([]byte, error) {
	if _, err := r.fd.Seek(offset, io.SeekStart); err != nil {
		return nil, errors.New("error in r.fd.Seek : " + err.Error())
	}
	r.reader.Reset(r.fd)

	compress, err := r.read(size)
	if err != nil {
		return nil, errors.New("error in read size : " + err.Error())
	}
	crc := binary.LittleEndian.Uint32(compress[size-4:])
	compressData := compress[:size-4]
	if utils.CompressedCheckSum(compressData) != crc {
		return nil, errors.New("error in check crc ")
	}

	data, err := snappy.Decode(nil, compressData)
	if err != nil {
		return nil, errors.New("error in snappy decode compressData")
	}
	return data, nil
}

func (r *SStReader) read(size int64) (b []byte, err error) {
	b = make([]byte, size)
	_, err = io.ReadFull(r.reader, b)
	return
}

func DecodeBlock(block []byte) ([]byte, []int) {
	n := len(block)
	nRestartPoint := int(binary.LittleEndian.Uint32(block[n-4:]))
	oRestartPoint := n - (nRestartPoint * 4) - 4
	restartPoint := make([]int, nRestartPoint)

	for i := 0; i < nRestartPoint; i++ {
		restartPoint[i] = int(binary.LittleEndian.Uint32(block[oRestartPoint+i*4:]))
	}
	return block[:oRestartPoint], restartPoint
}

func ReadRecord(prevKey []byte, buf *bytes.Buffer) ([]byte, []byte, error) {
	keyPrefixLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return nil, nil, errors.New("error in binary.ReadUvarint(buf) to get keyPrefixLen : " + err.Error())
	}

	keyLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return nil, nil, errors.New("error in binary.ReadUvarint(buf) to get keyLen : " + err.Error())
	}

	valueLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return nil, nil, errors.New("error in binary.ReadUvarint(buf) to get valueLen : " + err.Error())
	}

	key := make([]byte, keyLen)
	_, err = io.ReadFull(buf, key)
	if err != nil {
		return nil, nil, errors.New("error in read full from buf and key")
	}

	value := make([]byte, valueLen)
	_, err = io.ReadFull(buf, value)
	if err != nil {
		return nil, nil, errors.New("error in read full from buf and value")
	}

	actualKey := make([]byte, keyPrefixLen)
	copy(actualKey, prevKey[0:keyPrefixLen])
	actualKey = append(actualKey, key...)
	return actualKey, value, nil
}

func ReadFilter(index []byte) map[uint64][]byte {
	data, _ := DecodeBlock(index)
	buf := bytes.NewBuffer(data)

	filterMap := make(map[uint64][]byte)
	prevKey := make([]byte, 0)

	for {
		key, value, err := ReadRecord(prevKey, buf)
		if err != nil {
			panic(errors.New("error in readRecord(prevKey,buf) : " + err.Error()))
			break
		}

		offset, _ := binary.Uvarint(key)
		filterMap[offset] = value
		prevKey = key
	}
	return filterMap
}
func (r *SStReader) ReadFilter() (map[uint64][]byte, error) {
	if r.FilterOffset == 0 {
		if err := r.ReadFooter(); err != nil {
			return nil, err
		}
	}

	data, err := r.readBlock(r.FilterOffset, r.FilterSize)
	if err != nil {
		return nil, err
	}

	return ReadFilter(data), nil
}

func (r *SStReader) ReadIndex() ([]*Index, error) {
	if r.IndexOffset == 0 {
		if err := r.ReadFooter(); err != nil {
			return nil, err
		}
	}

	index, err := r.readBlock(r.IndexOffset, r.IndexSize)
	if err != nil {
		return nil, err
	}

	return ReadIndex(index), nil
}

func ReadIndex(index []byte) []*Index {
	data, _ := DecodeBlock(index)
	indexBuf := bytes.NewBuffer(data)

	indexes := make([]*Index, 0)
	prevKey := make([]byte, 0)

	for {
		key, value, err := ReadRecord(prevKey, indexBuf)
		if err != nil {
			panic(errors.New("error in readRecord(prevKey,indexBuf) : " + err.Error()))
			break
		}
		offset, n := binary.Uvarint(value)
		size, _ := binary.Uvarint(value[n:])

		indexes = append(indexes, &Index{
			Key:        key,
			PrevOffset: offset,
			PrevSize:   size,
		})
		prevKey = key
	}

	return indexes
}
