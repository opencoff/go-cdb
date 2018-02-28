package cdb_test

import (
	"testing"

	//"github.com/colinmarc/cdb"
	"cdb"
)

type kw struct {
	key string
	val string
}

var testRecords = []kw{
	{"hello", "world"},
	{"abc", "def"},
	{"123", "345"},
}

var invKeys = []string{
	"foo",
	"bar",
	"quux",
}

func TestGet(t *testing.T) {
	makeDB(t)

	db, err := cdb.Open("./test/test.cdb")
	if err != nil {
		t.Fatalf("Can't open newly created test.db: %s", err)
	}

	for _, r := range testRecords {
		v, err := db.Get([]byte(r.key))
		if err != nil {
			t.Fatalf("Can't find key %s: %s", r.key, err)
		}

		if r.val != string(v) {
			t.Fatalf("Value mismatch for key %s (exp %s, saw %s)", r.key, r.val, string(v))
		}
	}

	for _, r := range invKeys {
		v, err := db.Get([]byte(r))
		if err != nil {
			t.Fatalf("Read error for invalid key %s: %s", r, err)
		}

		if v != nil {
			t.Fatalf("Found unexpected key %s; val %s", r, string(v))
		}
	}
	db.Close()
}

func makeDB(t *testing.T) {
	db, err := cdb.Create("./test/test.cdb")
	if err != nil {
		t.Fatalf("Can't create test.cdb: %s", err)
	}

	for _, r := range testRecords {
		err = db.Put([]byte(r.key), []byte(r.val))
		if err != nil {
			t.Fatalf("Can't put key %s: %s", r.key, err)
		}
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Can't close test.db: %s", err)
	}
}
