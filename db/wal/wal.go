package wal

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/peterouob/gocloud/db/config"
	"os"
	"path/filepath"
	"sync"
)

type WALManager struct {
	conf       *config.Config
	mu         sync.Mutex
	w          *Writer
	r          *Reader
	fd         *os.File
	count      int
	maxWALSize int64
}

func NewWALManager(conf *config.Config, maxWALSize int64) (*WALManager, error) {

	b := new(bytes.Buffer)

	m := &WALManager{
		conf:       conf,
		w:          NewWriter(b),
		maxWALSize: maxWALSize,
	}

	if err := m.rotationWALFile(); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *WALManager) rotationWALFile() error {

	m.count++
	filename := fmt.Sprintf("wal_%d.log", m.count)
	path := filepath.Join(m.conf.Dir, filename)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to create WAL file: %v", err)
	}

	m.fd = file
	m.w = NewWriter(file)

	return nil
}

func (m *WALManager) LogWrite(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.w.Size() > m.maxWALSize {
		if err := m.rotationWALFile(); err != nil {
			return errors.New("error in rotation wal log file")
		}
	}

	writer := m.w.Next()

	buf := new(bytes.Buffer)
	buf.Write(key)

	_, err := writer.Write(buf.Bytes())
	if err != nil {
		return errors.New("failed to write to WAL : " + err.Error())
	}
	buf.Write(value)
	_, err = writer.Write(buf.Bytes())
	if err != nil {
		return errors.New("failed to write to WAL : " + err.Error())
	}
	m.w.Flush()
	return nil
}

//func (m *WALManager) WalNext() ([]byte, []byte, error) {
//	for {
//		if err := r.nextChunk(true); err == nil {
//			break
//		} else if !errors.Is(err, errSkip) {
//			return nil, nil, err
//		}
//	}
//	buf := bytes.NewBuffer(r.data)
//	key, value, err := readRecord(buf)
//	if err != nil {
//		return nil, nil, errors.New("error in read Record : " + err.Error())
//	}
//	return key, value, err
//}
//
//func readRecord(buf *bytes.Buffer) ([]byte, []byte, error) {
//	keyLen, err := binary.ReadUvarint(buf)
//	log.Println(keyLen)
//	if err != nil {
//		return nil, nil, errors.New("error in binary.ReadUvarint(buf) to get keyLen")
//	}
//	valueLen, err := binary.ReadUvarint(buf)
//	if err != nil {
//		return nil, nil, errors.New("error in binary.ReadUvarint(buf) to get valueLen")
//	}
//	log.Println(valueLen)
//
//	key := make([]byte, keyLen)
//	_, err = io.ReadFull(buf, key)
//	if err != nil {
//		return nil, nil, errors.New("error in io.ReadFull(buf,key)")
//	}
//
//	value := make([]byte, valueLen)
//	_, err = io.ReadFull(buf, value)
//	if err != nil {
//		return nil, nil, errors.New("error in io.ReadFull(buf,value)")
//	}
//
//	return key, value, nil
//}
