package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/golang/snappy"
	"github.com/peterouob/gocloud/db/config"
	"github.com/peterouob/gocloud/db/utils"
	"io"
	"log"
	"os"
	"path"
	"sync"
)

type SStReaderInterface interface {
	ReadFooter() error
	ReadBlock(uint64, uint64) ([]byte, error)
	ReadFilter() (map[uint64][]byte, error)
	ReadIndex() ([]*Index, error)
	Destroy()
}

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

var _ SStReaderInterface = (*SStReader)(nil)

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
	fileInfo, err := r.fd.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()

	if _, err := r.fd.Seek(fileSize-int64(r.conf.SstFooterSize), io.SeekStart); err != nil {
		return err
	}

	footerData := make([]byte, r.conf.SstFooterSize)
	if _, err := io.ReadFull(r.reader, footerData); err != nil {
		return err
	}

	buf := bytes.NewBuffer(footerData)

	filterOffset, err := binary.ReadUvarint(buf)
	if err != nil {
		return err
	}

	filterSize, err := binary.ReadUvarint(buf)
	if err != nil {
		return err
	}

	indexOffset, err := binary.ReadUvarint(buf)
	if err != nil {
		return err
	}

	indexSize, err := binary.ReadUvarint(buf)
	if err != nil {
		return err
	}

	if filterOffset == 0 || filterSize == 0 || indexOffset == 0 || indexSize == 0 {
		return errors.New("sst footer data error: zero values detected")
	}

	if filterOffset >= uint64(fileSize) || indexOffset >= uint64(fileSize) {
		return errors.New("sst footer data error: invalid offsets")
	}

	r.FilterOffset = int64(filterOffset)
	r.FilterSize = int64(filterSize)
	r.IndexOffset = int64(indexOffset)
	r.IndexSize = int64(indexSize)

	return nil

}

func (r *SStReader) ReadBlock(offset, size uint64) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	log.Printf("Reading block at offset %d, size %d", offset, size)

	if _, err := r.fd.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek error: %v", err)
	}

	compressed := make([]byte, size-4) // -4 for CRC
	if _, err := io.ReadFull(r.reader, compressed); err != nil {
		return nil, fmt.Errorf("read error: %v", err)
	}

	crc := make([]byte, 4)
	if _, err := io.ReadFull(r.reader, crc); err != nil {
		return nil, fmt.Errorf("crc read error: %v", err)
	}

	expectedCRC := binary.LittleEndian.Uint32(crc)
	actualCRC := utils.CompressedCheckSum(compressed)

	if expectedCRC != actualCRC {
		return nil, fmt.Errorf("CRC mismatch: expected %d, got %d", expectedCRC, actualCRC)
	}

	decompressed, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, fmt.Errorf("decompress error: %v", err)
	}

	log.Printf("Successfully read block: %d bytes compressed, %d bytes decompressed",
		len(compressed), len(decompressed))

	return decompressed, nil
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

func DecodeBlock(block []byte) ([]byte, []int, error) {
	n := len(block)
	log.Println("block", string(block[:n-4]))
	nRestartPoint := int(binary.LittleEndian.Uint32(block[n-4:]))
	oRestartPoint := n - (nRestartPoint * 4) - 4
	restartPoint := make([]int, nRestartPoint)
	log.Println("nRestartPoint:", nRestartPoint)
	for i := 0; i < nRestartPoint; i++ {
		restartPoint[i] = int(binary.LittleEndian.Uint32(block[oRestartPoint+i*4:]))
	}
	return block[:oRestartPoint], restartPoint, nil
}

func ReadRecord(prevKey []byte, buf *bytes.Buffer) ([]byte, []byte, error) {
	if buf.Len() == 0 {
		return nil, nil, io.EOF
	}

	keyPrefixLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading keyPrefixLen: %v", err)
	}

	if keyPrefixLen > uint64(len(prevKey)) {
		return nil, nil, fmt.Errorf("invalid prefix length: %d > %d", keyPrefixLen, len(prevKey))
	}

	keyLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading keyLen: %v", err)
	}

	valueLen, err := binary.ReadUvarint(buf)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading valueLen: %v", err)
	}

	if keyLen > uint64(buf.Len()) || valueLen > uint64(buf.Len()-int(keyLen)) {
		return nil, nil, fmt.Errorf("invalid key/value length")
	}

	keySuffix := make([]byte, keyLen)
	if _, err := io.ReadFull(buf, keySuffix); err != nil {
		return nil, nil, fmt.Errorf("error reading key: %v", err)
	}

	value := make([]byte, valueLen)
	if _, err := io.ReadFull(buf, value); err != nil {
		return nil, nil, fmt.Errorf("error reading value: %v", err)
	}

	key := make([]byte, keyPrefixLen+keyLen)
	copy(key[:keyPrefixLen], prevKey[:keyPrefixLen])
	copy(key[keyPrefixLen:], keySuffix)

	return key, value, nil
}

func ReadFilter(index []byte) map[uint64][]byte {
	data, _, err := DecodeBlock(index)
	if err != nil {
		log.Printf("error in DecodeBlock: %v", err)
		return nil
	}
	buf := bytes.NewBuffer(data)

	filterMap := make(map[uint64][]byte)
	prevKey := make([]byte, 0)

	for {
		key, value, err := ReadRecord(prevKey, buf)
		if err != nil {
			panic(errors.New("error in readRecord(prvKey,buf) : " + err.Error()))
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
	data, _, err := DecodeBlock(index)
	if err != nil {
		log.Printf("error in DecodeBlock: %v", err)
		return nil
	}
	indexBuf := bytes.NewBuffer(data)

	indexes := make([]*Index, 0)
	prevKey := make([]byte, 0)

	for {
		key, value, err := ReadRecord(prevKey, indexBuf)
		if err != nil {
			fmt.Errorf("error in readRecord(prvKey,indexBuf): %s\n" + err.Error())
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

func (r *SStReader) Destroy() {
	r.reader.Reset(r.fd)
	if err := r.fd.Close(); err != nil {
		panic(errors.New("error in close fd : " + err.Error()))
	}
	if err := os.Remove(r.fd.Name()); err != nil {
		panic(errors.New("error in remove file : " + err.Error()))
	}
}

type SsWriterInterface interface {
	Append([]byte, []byte)
	Finish() (int64, map[uint64][]byte, []*Index, error)
	Size() int
	Close()
}

type SsWriter struct {
	conf            *config.Config
	fd              *os.File
	dataBuf         *bytes.Buffer
	fileBuf         *bytes.Buffer
	indexBuf        *bytes.Buffer
	index           []*Index
	filter          map[uint64][]byte
	bf              *utils.BloomFilter
	dataBlock       *Block
	filterBlock     *Block
	indexBlock      *Block
	indexScratch    [20]byte
	prevKey         []byte
	prevBlockOffset uint64
	prevBlockSize   uint64
}

var _ SsWriterInterface = (*SsWriter)(nil)

func NewSStWriter(file string, conf *config.Config) (*SsWriter, error) {
	fd, err := os.OpenFile(path.Join(conf.Dir, file), os.O_RDWR|os.O_CREATE, 0777)
	if os.IsNotExist(err) {
		fd, err = os.Create(path.Join(conf.Dir, file))
		if err != nil {
			return nil, errors.New("error in create file : " + err.Error())
		}
	}
	if err != nil {
		return nil, errors.New("create file error :" + err.Error())
	}
	return &SsWriter{
		conf:        conf,
		fd:          fd,
		dataBuf:     bytes.NewBuffer(make([]byte, 0)),
		fileBuf:     bytes.NewBuffer(make([]byte, 0)),
		indexBuf:    bytes.NewBuffer(make([]byte, 0)),
		index:       make([]*Index, 0),
		filter:      make(map[uint64][]byte),
		bf:          utils.NewBloomFilter(10),
		dataBlock:   NewBlock(conf),
		filterBlock: NewBlock(conf),
		indexBlock:  NewBlock(conf),
		prevKey:     make([]byte, 0),
	}, nil
}

type Index struct {
	Key        []byte
	PrevOffset uint64
	PrevSize   uint64
}

func (w *SsWriter) Append(key, value []byte) {
	if w.dataBlock.n == 0 {
		skey := make([]byte, len(key))
		copy(skey, key)
		w.addIndex(skey)
	}

	w.dataBlock.Append(key, value)
	w.bf.Add(key)
	w.prevKey = key

	if w.dataBlock.Size() > w.conf.SstDataBlockSize {
		w.flushBlock()
	}
}

func (w *SsWriter) addIndex(key []byte) {
	n := binary.PutUvarint(w.indexScratch[0:], w.prevBlockOffset)
	n += binary.PutUvarint(w.indexScratch[n:], w.prevBlockSize)
	separator := GetSeparator(w.prevKey, key)

	w.indexBlock.Append(separator, w.indexScratch[:n])
	w.index = append(w.index, &Index{Key: separator, PrevOffset: w.prevBlockOffset, PrevSize: w.prevBlockSize})
}

func (w *SsWriter) flushBlock() {
	w.prevBlockOffset = uint64(w.dataBuf.Len())
	n := binary.PutUvarint(w.indexScratch[0:], w.prevBlockOffset)

	filter := w.bf.Hash()
	w.filter[w.prevBlockOffset] = filter
	w.filterBlock.Append(w.indexScratch[:n], filter)
	w.bf.Reset()

	var err error
	w.prevBlockSize, err = w.dataBlock.FlushBlockTo(w.dataBuf)
	if err != nil {
		log.Printf("Error flushing block: %v", err)
		panic("write block error :" + err.Error())
	}

}

func (w *SsWriter) Finish() (int64, map[uint64][]byte, []*Index, error) {
	if w.bf.KeyLen() > 0 {
		w.flushBlock()
	}

	dataSize := int64(w.dataBuf.Len())
	if _, err := w.fd.Write(w.dataBuf.Bytes()); err != nil {
		return 0, nil, nil, err
	}

	filterOffset := dataSize
	filterSize, err := w.filterBlock.FlushBlockTo(w.fileBuf)
	if err != nil {
		return 0, nil, nil, err
	}
	if _, err := w.fd.Write(w.fileBuf.Bytes()); err != nil {
		return 0, nil, nil, err
	}

	indexOffset := filterOffset + int64(w.fileBuf.Len())
	w.addIndex(w.prevKey)
	indexSize, err := w.indexBlock.FlushBlockTo(w.indexBuf)
	if err != nil {
		return 0, nil, nil, err
	}
	if _, err := w.fd.Write(w.indexBuf.Bytes()); err != nil {
		return 0, nil, nil, err
	}

	footer := make([]byte, w.conf.SstFooterSize)
	var n int
	n = binary.PutUvarint(footer[0:], uint64(filterOffset))
	n += binary.PutUvarint(footer[n:], filterSize)
	n += binary.PutUvarint(footer[n:], uint64(indexOffset))
	n += binary.PutUvarint(footer[n:], indexSize)

	if _, err := w.fd.Write(footer); err != nil {
		return 0, nil, nil, err
	}

	totalSize := indexOffset + int64(w.indexBuf.Len()) + int64(w.conf.SstFooterSize)

	log.Printf("Final file layout:")
	log.Printf("Data blocks: 0 to %d", dataSize)
	log.Printf("Filter blocks: %d to %d", filterOffset, filterOffset+int64(w.fileBuf.Len()))
	log.Printf("Index blocks: %d to %d", indexOffset, indexOffset+int64(w.indexBuf.Len()))
	log.Printf("Footer: %d to %d", totalSize-int64(w.conf.SstFooterSize), totalSize)

	if err := w.verifyFooter(); err != nil {
		return 0, nil, nil, errors.New("footer verification failed" + err.Error())
	}

	return totalSize, w.filter, w.index, nil
}

func GetSeparator(a, b []byte) []byte {
	if len(a) == 0 {
		n := len(b) - 1
		c := b[n] - 1
		return append(b[0:n], c)
	}

	n := SharedPrefixLen(a, b)
	if n == 0 || n == len(a) {
		return a
	} else {
		c := a[n] + 1
		return append(a[0:n], c)
	}
}

func (w *SsWriter) Size() int {
	return w.dataBuf.Len()
}

func (w *SsWriter) Close() {
	if err := w.fd.Close(); err != nil {
		panic(errors.New("error in close w.fd : " + err.Error()))
	}
	w.dataBuf.Reset()
	w.indexBuf.Reset()
}

func SharedPrefixLen(a, b []byte) int {
	i, n := 0, len(a)
	if n > len(b) {
		n = len(b)
	}
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

func (w *SsWriter) verifyFooter() error {
	if _, err := w.fd.Seek(-int64(w.conf.SstFooterSize), io.SeekEnd); err != nil {
		return err
	}
	footerData := make([]byte, w.conf.SstFooterSize)
	if _, err := io.ReadFull(bufio.NewReader(w.fd), footerData); err != nil {
		return errors.New("error in footer data : " + err.Error())
	}

	buf := bytes.NewBuffer(footerData)
	filterOffset, _ := binary.ReadUvarint(buf)
	filterSize, _ := binary.ReadUvarint(buf)
	indexOffset, _ := binary.ReadUvarint(buf)
	indexSize, _ := binary.ReadUvarint(buf)

	log.Printf("Footer verification - FilterOffset: %d, FilterSize: %d, IndexOffset: %d, IndexSize: %d",
		filterOffset, filterSize, indexOffset, indexSize)

	return nil
}
