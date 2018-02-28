// Package cdb provides a native implementation of cdb, a constant key/value
// database with some very nice properties.
//
// For more information on cdb, see the original design doc at http://cr.yp.to/cdb.html.
//
package cdb

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"os"

	// Sudhi's utility library
	"github.com/opencoff/go-lib/util"
)

const indexSize = 256 * 8

type index [256]table

// CDB represents an open CDB database. It can only be used for reads; to
// create a database, use Writer.
type CDB struct {
	reader io.ReaderAt
	hasher func(b []byte) uint32
	index  index
}

type table struct {
	offset uint32
	length uint32
}

// Open opens an existing CDB database at the given path.
func Open(path string) (*CDB, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	err = verifyChecksum(f, path)
	if err != nil {
		return nil, err
	}

	return New(f, nil)
}

// Verify the DB integrity
// The last 32 bytes of the DB are the SHA256 checksum of the bytes
// preceding it. It was appended by the writer module.
// XXX This is a non-standard extension to CDB;
func verifyChecksum(f *os.File, path string) error {
	st, err := f.Stat()
	if err != nil {
		return fmt.Errorf("can't stat %s: %s", path, err)
	}

	sz := st.Size()
	if sz < (2048 + sha256.Size) {
		return fmt.Errorf("cdb file %s too small", path)
	}

	datasz := sz - sha256.Size

	// Skip to just before the checksum
	_, err = f.Seek(datasz, os.SEEK_SET)
	if err != nil {
		return fmt.Errorf("can't seek %s: %s", path, err)
	}

	var eck [sha256.Size]byte

	n, err := f.Read(eck[:])
	if err != nil {
		return fmt.Errorf("can't read checksum of %s: %s", path, err)
	}

	if n != sha256.Size {
		return fmt.Errorf("i/o error while reading checksum of %s: only read %d bytes", n)
	}

	// Verify checksum now
	hh := sha256.New()
	err = util.MmapReader(f, 0, datasz, hh)
	if err != nil {
		return fmt.Errorf("i/o error during checksum calculation: %s", err)
	}

	ck := hh.Sum(nil)

	if 1 != subtle.ConstantTimeCompare(eck[:], ck) {
		return fmt.Errorf("checksum failed. DB possibly corrupt!")
	}

	// rewind to start of file
	_, err = f.Seek(0, os.SEEK_SET)
	if err != nil {
		return fmt.Errorf("can't seek %s: %s", path, err)
	}

	return nil
}

// New opens a new CDB instance for the given io.ReaderAt. It can only be used
// for reads; to create a database, use Writer.
//
// If hasher is nil, it will default to the CDB hash function. If a database
// was created with a particular hash function, that same hash function must be
// passed to New, or the database will return incorrect results.
func New(reader io.ReaderAt, hasher hash.Hash32) (*CDB, error) {
	var hf func(b []byte) uint32 = Hash32
	if hasher != nil {
		hf = func(b []byte) uint32 {
			hasher.Reset()
			hasher.Write(b)
			return hasher.Sum32()
		}
	}

	cdb := &CDB{reader: reader, hasher: hf}
	err := cdb.readIndex()
	if err != nil {
		return nil, err
	}

	return cdb, nil
}

// Get returns the value for a given key, or nil if it can't be found.
func (cdb *CDB) Get(key []byte) ([]byte, error) {
	hash := cdb.hasher(key)

	table := cdb.index[hash&0xff]
	if table.length == 0 {
		return nil, nil
	}

	// Probe the given hash table, starting at the given slot.
	startingSlot := (hash >> 8) % table.length
	slot := startingSlot

	for {
		slotOffset := table.offset + (8 * slot)
		slotHash, offset, err := readTuple(cdb.reader, slotOffset)
		if err != nil {
			return nil, err
		}

		// An empty slot means the key doesn't exist.
		if slotHash == 0 {
			break
		} else if slotHash == hash {
			value, err := cdb.getValueAt(offset, key)
			if err != nil {
				return nil, err
			} else if value != nil {
				return value, nil
			}
		}

		slot = (slot + 1) % table.length
		if slot == startingSlot {
			break
		}
	}

	return nil, nil
}

// Close closes the database to further reads.
func (cdb *CDB) Close() error {
	if closer, ok := cdb.reader.(io.Closer); ok {
		return closer.Close()
	} else {
		return nil
	}
}

func (cdb *CDB) readIndex() error {
	buf := make([]byte, indexSize)
	_, err := cdb.reader.ReadAt(buf, 0)
	if err != nil {
		return err
	}

	for i := 0; i < 256; i++ {
		off := i * 8
		cdb.index[i] = table{
			offset: binary.LittleEndian.Uint32(buf[off : off+4]),
			length: binary.LittleEndian.Uint32(buf[off+4 : off+8]),
		}
	}

	return nil
}

func (cdb *CDB) getValueAt(offset uint32, expectedKey []byte) ([]byte, error) {
	keyLength, valueLength, err := readTuple(cdb.reader, offset)
	if err != nil {
		return nil, err
	}

	// We can compare key lengths before reading the key at all.
	if int(keyLength) != len(expectedKey) {
		return nil, nil
	}

	buf := make([]byte, keyLength+valueLength)
	_, err = cdb.reader.ReadAt(buf, int64(offset+8))
	if err != nil {
		return nil, err
	}

	// If they keys don't match, this isn't it.
	if bytes.Compare(buf[:keyLength], expectedKey) != 0 {
		return nil, nil
	}

	return buf[keyLength:], nil
}
