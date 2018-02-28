package cdb

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"math"
	"os"
	"sync"

	// Sudhi's utility library
	"github.com/opencoff/go-lib/util"
)

var ErrTooMuchData = errors.New("CDB files are limited to 4GB of data")

// Writer provides an API for creating a CDB database record by record.
//
// Close or Freeze must be called to finalize the database, or the resulting
// file will be invalid.
type Writer struct {
	hasher       func(b []byte) uint32
	writer       *os.File
	entries      [256][]entry
	finalizeOnce sync.Once

	bufferedWriter      *bufio.Writer
	bufferedOffset      int64
	estimatedFooterSize int64
}

type entry struct {
	hash   uint32
	offset uint32
}

// Create opens a CDB database at the given path. If the file exists, it will
// be overwritten.
func Create(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	return NewWriter(f, nil)
}

// NewWriter opens a CDB database for the given io.WriteSeeker.
//
// If hasher is nil, it will default to the CDB hash function.
func NewWriter(writer *os.File, hasher hash.Hash32) (*Writer, error) {
	// Leave 256 * 8 bytes for the index at the head of the file.
	_, err := writer.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	_, err = writer.Write(make([]byte, indexSize))
	if err != nil {
		return nil, err
	}

	var hf func(b []byte) uint32 = Hash32
	if hasher != nil {
		hf = func(b []byte) uint32 {
			hasher.Reset()
			hasher.Write(b)
			return hasher.Sum32()
		}
	}

	return &Writer{
		hasher:         hf,
		writer:         writer,
		bufferedWriter: bufio.NewWriterSize(writer, 65536),
		bufferedOffset: indexSize,
	}, nil
}

// Put adds a key/value pair to the database. If the amount of data written
// would exceed the limit, Put returns ErrTooMuchData.
func (cdb *Writer) Put(key, value []byte) error {
	entrySize := int64(8 + len(key) + len(value))
	if (cdb.bufferedOffset + entrySize + cdb.estimatedFooterSize + 16) > math.MaxUint32 {
		return ErrTooMuchData
	}

	// Record the entry in the hash table, to be written out at the end.
	hash := cdb.hasher(key)
	table := hash & 0xff

	entry := entry{hash: hash, offset: uint32(cdb.bufferedOffset)}
	cdb.entries[table] = append(cdb.entries[table], entry)

	// Write the key length, then value length, then key, then value.
	err := writeTuple(cdb.bufferedWriter, uint32(len(key)), uint32(len(value)))
	if err != nil {
		return err
	}

	_, err = cdb.bufferedWriter.Write(key)
	if err != nil {
		return err
	}

	_, err = cdb.bufferedWriter.Write(value)
	if err != nil {
		return err
	}

	cdb.bufferedOffset += entrySize
	cdb.estimatedFooterSize += 16
	return nil
}

// Close finalizes the database, then closes it to further writes.
//
// Close or Freeze must be called to finalize the database, or the resulting
// file will be invalid.
func (cdb *Writer) Close() error {
	var err error
	cdb.finalizeOnce.Do(func() {
		_, err = cdb.finalize()
	})

	if err != nil {
		return err
	}

	return cdb.writer.Close()
}

// Freeze finalizes the database, then opens it for reads. If the stream cannot
// be converted to a io.ReaderAt, Freeze will return os.ErrInvalid.
//
// Close or Freeze must be called to finalize the database, or the resulting
// file will be invalid.
func (cdb *Writer) Freeze() (*CDB, error) {
	var err error
	var index index
	cdb.finalizeOnce.Do(func() {
		index, err = cdb.finalize()
	})

	if err != nil {
		return nil, err
	}

	readerAt := cdb.writer
	return &CDB{reader: readerAt, index: index, hasher: cdb.hasher}, nil
}

func (cdb *Writer) finalize() (index, error) {
	var index index

	// Write the hashtables out, one by one, at the end of the file.
	for i := 0; i < 256; i++ {
		tableEntries := cdb.entries[i]
		tableSize := uint32(len(tableEntries) << 1)

		index[i] = table{
			offset: uint32(cdb.bufferedOffset),
			length: tableSize,
		}

		sorted := make([]entry, tableSize)
		for _, entry := range tableEntries {
			slot := (entry.hash >> 8) % tableSize

			for {
				if sorted[slot].hash == 0 {
					sorted[slot] = entry
					break
				}

				slot = (slot + 1) % tableSize
			}
		}

		for _, entry := range sorted {
			err := writeTuple(cdb.bufferedWriter, entry.hash, entry.offset)
			if err != nil {
				return index, err
			}

			cdb.bufferedOffset += 8
			if cdb.bufferedOffset > math.MaxUint32 {
				return index, ErrTooMuchData
			}
		}
	}

	// We're done with the buffer.
	err := cdb.bufferedWriter.Flush()
	cdb.bufferedWriter = nil
	if err != nil {
		return index, err
	}

	// Seek to the beginning of the file and write out the index.
	_, err = cdb.writer.Seek(0, os.SEEK_SET)
	if err != nil {
		return index, err
	}

	buf := make([]byte, indexSize)
	for i, table := range index {
		off := i * 8
		binary.LittleEndian.PutUint32(buf[off:off+4], table.offset)
		binary.LittleEndian.PutUint32(buf[off+4:off+8], table.length)
	}

	_, err = cdb.writer.Write(buf)
	if err != nil {
		return index, err
	}

	// Finally calculate a checksum and append it to the end of the
	// file
	hh := sha256.New()

	var sz int64

	// Go to the end to append the checksum. As a consequence, we
	// also get the current file size.
	sz, err = cdb.writer.Seek(0, os.SEEK_END)
	if err != nil {
		return index, err
	}

	err = util.MmapReader(cdb.writer, 0, sz, hh)
	if err != nil {
		return index, err
	}

	ck := hh.Sum(nil)

	_, err = cdb.writer.Write(ck)
	if err != nil {
		return index, err
	}

	return index, nil
}
