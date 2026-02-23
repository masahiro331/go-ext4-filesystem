package ext4

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func buildDirEntry(inode uint32, name string, flags uint8) []byte {
	nameBytes := []byte(name)
	nameLen := uint8(len(nameBytes))
	recLen := uint16(8 + len(nameBytes))
	// align to 4 bytes
	if recLen%4 != 0 {
		recLen += 4 - recLen%4
	}

	buf := make([]byte, recLen)
	binary.LittleEndian.PutUint32(buf[0:4], inode)
	binary.LittleEndian.PutUint16(buf[4:6], recLen)
	buf[6] = nameLen
	buf[7] = flags
	copy(buf[8:], nameBytes)
	return buf
}

func TestExtractDirectoryEntriesSkipsInodeZero(t *testing.T) {
	var data []byte
	data = append(data, buildDirEntry(10, "valid_file", 1)...)
	data = append(data, buildDirEntry(0, "deleted_file", 1)...)
	data = append(data, buildDirEntry(20, "another_file", 2)...)

	entries, err := extractDirectoryEntries(bytes.NewBuffer(data))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].Name != "valid_file" {
		t.Errorf("expected first entry 'valid_file', got %q", entries[0].Name)
	}
	if entries[1].Name != "another_file" {
		t.Errorf("expected second entry 'another_file', got %q", entries[1].Name)
	}
}

func TestExtractDirectoryEntriesSkipsDotEntries(t *testing.T) {
	var data []byte
	data = append(data, buildDirEntry(2, ".", 2)...)
	data = append(data, buildDirEntry(2, "..", 2)...)
	data = append(data, buildDirEntry(11, "real", 1)...)

	entries, err := extractDirectoryEntries(bytes.NewBuffer(data))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Name != "real" {
		t.Errorf("expected 'real', got %q", entries[0].Name)
	}
}

func TestExtractDirectoryEntriesSkipsDeletedChecksum(t *testing.T) {
	var data []byte
	data = append(data, buildDirEntry(5, "keep", 1)...)
	data = append(data, buildDirEntry(6, "csum", 0xDE)...)
	data = append(data, buildDirEntry(7, "notype", 0)...)

	entries, err := extractDirectoryEntries(bytes.NewBuffer(data))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Name != "keep" {
		t.Errorf("expected 'keep', got %q", entries[0].Name)
	}
}
