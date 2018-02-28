package cdb

import (

	//"github.com/dchest/siphash"
	"github.com/opencoff/go-lib/fasthash"
)

//var seed = []byte{0x2d, 0xe9, 0xce, 0x7b, 0x97, 0x7e, 0x79, 0xd9, 0x56, 0xc6, 0x9f, 0x68, 0x0c, 0x8f, 0x66, 0x7b}

// This is all that is needed
func Hash32(key []byte) uint32 {
	h := fasthash.Hash64(0x2de9ce7b97d9569f, key)
	return uint32(h - h>>32)
}
