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

// TestGetInode_SmallInodeSize verifies that getInode() correctly reads
// inodes when InodeSize is smaller than the Go Inode struct (256 bytes).
// With InodeSize=128 (ext2/ext3), every 4th inode in a sector would
// previously fail with EOF because only 512 bytes were read.
func TestGetInode_SmallInodeSize(t *testing.T) {
	tests := []struct {
		name      string
		inodeSize uint16
		numInodes int // number of inodes to test within one group
	}{
		{
			name:      "ext2/ext3: InodeSize=128, 4 inodes per sector",
			inodeSize: 128,
			numInodes: 8, // 2 sectors worth
		},
		{
			name:      "ext4: InodeSize=256, 2 inodes per sector",
			inodeSize: 256,
			numInodes: 4, // 2 sectors worth
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const blockSize int64 = 4096
			const inodeTableBlock = 1 // inode table starts at block 1

			// Build an image with inode table data.
			// Each inode on disk is InodeSize bytes. We place a marker in the
			// Mode field (offset 0) of each inode.
			imageSize := blockSize * 2 // block 0 unused, block 1 = inode table
			image := make([]byte, imageSize)

			for i := 0; i < tt.numInodes; i++ {
				offset := int(blockSize)*inodeTableBlock + i*int(tt.inodeSize)
				marker := uint16(0x8000 + i) // distinct Mode value per inode
				binary.LittleEndian.PutUint16(image[offset:offset+2], marker)
			}

			r := io.NewSectionReader(bytes.NewReader(image), 0, int64(imageSize))

			gd := GroupDescriptor{}
			gd.InodeTableLo = inodeTableBlock

			fs := &FileSystem{
				r: r,
				sb: Superblock{
					LogBlockSize:  2, // 4096
					InodeSize:     tt.inodeSize,
					InodePerGroup: uint32(tt.numInodes),
				},
				gds:   []GroupDescriptor{gd},
				cache: &mockCache[string, any]{},
			}

			for i := 0; i < tt.numInodes; i++ {
				inodeAddr := int64(i + 1) // inode numbers are 1-based
				inode, err := fs.getInode(inodeAddr)
				if err != nil {
					t.Fatalf("getInode(%d) error: %v (InodeSize=%d, offset within sector=%d)",
						inodeAddr, err, tt.inodeSize, (i*int(tt.inodeSize))%SectorSize)
				}
				wantMode := uint16(0x8000 + i)
				if inode.Mode != wantMode {
					t.Errorf("inode %d: Mode=%#x, want %#x", inodeAddr, inode.Mode, wantMode)
				}
			}
		})
	}
}

// TestGetInode_SmallInodeSizeExtendedFieldsZero verifies that when
// InodeSize < Go struct size, extended fields are zero-initialized.
func TestGetInode_SmallInodeSizeExtendedFieldsZero(t *testing.T) {
	const blockSize int64 = 4096
	const inodeTableBlock = 1
	const inodeSize = 128

	imageSize := blockSize * 2
	image := make([]byte, imageSize)

	// Write a valid inode at index 0 with Mode = regular file
	offset := int(blockSize) * inodeTableBlock
	binary.LittleEndian.PutUint16(image[offset:offset+2], 0x8180) // Mode: regular + 0o600

	// Write garbage at byte 128+ (this is the NEXT inode's space, not ours)
	for i := offset + inodeSize; i < offset+256 && i < len(image); i++ {
		image[i] = 0xFF
	}

	r := io.NewSectionReader(bytes.NewReader(image), 0, int64(imageSize))
	gd := GroupDescriptor{}
	gd.InodeTableLo = inodeTableBlock

	fs := &FileSystem{
		r: r,
		sb: Superblock{
			LogBlockSize:  2,
			InodeSize:     inodeSize,
			InodePerGroup: 16,
		},
		gds:   []GroupDescriptor{gd},
		cache: &mockCache[string, any]{},
	}

	inode, err := fs.getInode(1)
	if err != nil {
		t.Fatalf("getInode(1) error: %v", err)
	}

	// Mode should be read correctly
	if inode.Mode != 0x8180 {
		t.Errorf("Mode = %#x, want %#x", inode.Mode, 0x8180)
	}

	// Extended fields (beyond 128 bytes) should be zero, NOT 0xFF garbage
	if inode.ExtraIsize != 0 {
		t.Errorf("ExtraIsize = %d, want 0 (should not read adjacent inode data)", inode.ExtraIsize)
	}
	if inode.CtimeExtra != 0 {
		t.Errorf("CtimeExtra = %#x, want 0", inode.CtimeExtra)
	}
}
