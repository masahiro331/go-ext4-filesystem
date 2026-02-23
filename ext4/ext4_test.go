package ext4

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

// TestExtents_InternalNodeUsesFullBlockSize verifies that extents() reads
// the full block size (not SectorSize) when following internal extent nodes.
// With a 4096-byte block, a leaf node can hold up to 340 extents, but if
// only 512 bytes were read, only 41 would be visible.
func TestExtents_InternalNodeUsesFullBlockSize(t *testing.T) {
	const blockSize = 4096
	const leafBlock = 1 // leaf node at block 1

	// Number of leaf extents: 50 exceeds SectorSize (512B) capacity of 41.
	const numExtents = 50

	// Build the leaf block: ExtentHeader (depth=0) + numExtents Extent entries
	leafBuf := make([]byte, blockSize)
	leafWriter := bytes.NewBuffer(leafBuf[:0])
	binary.Write(leafWriter, binary.LittleEndian, ExtentHeader{
		Magic:   0xF30A,
		Entries: numExtents,
		Max:     340,
		Depth:   0,
	})
	for i := uint32(0); i < numExtents; i++ {
		binary.Write(leafWriter, binary.LittleEndian, Extent{
			Block:   i * 10,
			Len:     1,
			StartHi: 0,
			StartLo: 100 + i,
		})
	}
	copy(leafBuf, leafWriter.Bytes())

	// Build the image: block 0 is unused, block 1 is the leaf node
	imageSize := blockSize * 2
	image := make([]byte, imageSize)
	copy(image[blockSize:], leafBuf)

	r := io.NewSectionReader(bytes.NewReader(image), 0, int64(imageSize))
	fs := &FileSystem{
		r: r,
		sb: Superblock{
			LogBlockSize: 2, // 1024 << 2 = 4096
		},
	}

	// Build root extent data: header (depth=1, 1 entry) + 1 internal entry
	rootBuf := &bytes.Buffer{}
	binary.Write(rootBuf, binary.LittleEndian, ExtentHeader{
		Magic:   0xF30A,
		Entries: 1,
		Max:     4,
		Depth:   1,
	})
	binary.Write(rootBuf, binary.LittleEndian, ExtentInternal{
		Block:   0,
		LeafLow: leafBlock,
	})

	extents, err := fs.extents(rootBuf.Bytes(), nil)
	if err != nil {
		t.Fatalf("extents() error: %v", err)
	}
	if len(extents) != numExtents {
		t.Errorf("got %d extents, want %d (buffer may be too small)", len(extents), numExtents)
	}
}
