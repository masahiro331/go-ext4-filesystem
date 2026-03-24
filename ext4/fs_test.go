package ext4

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
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

func TestExtractDirectoryEntriesSkipsChecksumTail(t *testing.T) {
	// Checksum tail entries have inode=0 and file_type=0xDE; skipped by inode==0 guard
	var data []byte
	data = append(data, buildDirEntry(5, "keep", 1)...)
	data = append(data, buildDirEntry(0, "csum", 0xDE)...)

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

func TestExtractDirectoryEntriesKeepsUnknownFileType(t *testing.T) {
	// file_type=0 (EXT4_FT_UNKNOWN) is valid on filesystems without FEATURE_INCOMPAT_FILETYPE
	var data []byte
	data = append(data, buildDirEntry(5, "known", 1)...)
	data = append(data, buildDirEntry(7, "unknown_type", 0)...)

	entries, err := extractDirectoryEntries(bytes.NewBuffer(data))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if entries[0].Name != "known" {
		t.Errorf("expected 'known', got %q", entries[0].Name)
	}
	if entries[1].Name != "unknown_type" {
		t.Errorf("expected 'unknown_type', got %q", entries[1].Name)
	}
}

func TestParseDxBlockNumbers(t *testing.T) {
	tests := []struct {
		name   string
		count  uint16
		data   []byte
		expect []uint32
	}{
		{
			name:  "single entry (header only)",
			count: 1,
			data: func() []byte {
				b := make([]byte, 4)
				binary.LittleEndian.PutUint32(b, 5)
				return b
			}(),
			expect: []uint32{5},
		},
		{
			name:  "three entries",
			count: 3,
			data: func() []byte {
				// block0(4) + entry1(hash:4+block:4) + entry2(hash:4+block:4)
				b := make([]byte, 4+8+8)
				binary.LittleEndian.PutUint32(b[0:], 10)   // block0
				binary.LittleEndian.PutUint32(b[4:], 100)  // hash1
				binary.LittleEndian.PutUint32(b[8:], 20)   // block1
				binary.LittleEndian.PutUint32(b[12:], 200) // hash2
				binary.LittleEndian.PutUint32(b[16:], 30)  // block2
				return b
			}(),
			expect: []uint32{10, 20, 30},
		},
		{
			name:   "empty data",
			count:  1,
			data:   []byte{},
			expect: []uint32{},
		},
		{
			name:   "count zero",
			count:  0,
			data:   make([]byte, 32),
			expect: []uint32{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseDxBlockNumbers(tt.data, tt.count)
			if len(got) != len(tt.expect) {
				t.Fatalf("expected %d blocks, got %d", len(tt.expect), len(got))
			}
			for i := range tt.expect {
				if got[i] != tt.expect[i] {
					t.Errorf("block[%d] = %d, want %d", i, got[i], tt.expect[i])
				}
			}
		})
	}
}

// buildHTreeRootBlock constructs an HTree root block with the given parameters.
func buildHTreeRootBlock(blockSize int, indirectLevels uint8, leafBlocks []uint32) []byte {
	block := make([]byte, blockSize)

	// dot entry: inode=2, rec_len=12, name_len=1, file_type=2
	binary.LittleEndian.PutUint32(block[0x00:], 2)
	binary.LittleEndian.PutUint16(block[0x04:], 12)
	block[0x06] = 1
	block[0x07] = 2
	block[0x08] = '.'

	// dotdot entry: inode=2, rec_len=blockSize-12, name_len=2, file_type=2
	binary.LittleEndian.PutUint32(block[0x0C:], 2)
	binary.LittleEndian.PutUint16(block[0x10:], uint16(blockSize-12))
	block[0x12] = 2
	block[0x13] = 2
	block[0x14] = '.'
	block[0x15] = '.'

	// dx_root_info at 0x18
	binary.LittleEndian.PutUint32(block[0x18:], 0) // reserved_zero
	block[0x1C] = 1                                // hash_version
	block[0x1D] = 8                                // info_length
	block[0x1E] = indirectLevels
	block[0x1F] = 0

	// DxCountLimit at 0x20
	count := uint16(len(leafBlocks))
	binary.LittleEndian.PutUint16(block[0x20:], 100) // limit
	binary.LittleEndian.PutUint16(block[0x22:], count)

	// dx_entries starting at 0x24
	// header entry: block0
	if len(leafBlocks) > 0 {
		binary.LittleEndian.PutUint32(block[0x24:], leafBlocks[0])
	}
	// remaining entries: hash + block
	for i := 1; i < len(leafBlocks); i++ {
		off := 0x28 + (i-1)*8
		binary.LittleEndian.PutUint32(block[off:], uint32(i*0x1000)) // hash
		binary.LittleEndian.PutUint32(block[off+4:], leafBlocks[i])  // block
	}

	return block
}

// buildHTreeInternalNode constructs an HTree internal node block.
func buildHTreeInternalNode(blockSize int, childBlocks []uint32) []byte {
	block := make([]byte, blockSize)

	// fake dirent: inode=0, rec_len=blockSize, name_len=0, file_type=0
	binary.LittleEndian.PutUint32(block[0x00:], 0)
	binary.LittleEndian.PutUint16(block[0x04:], uint16(blockSize))
	block[0x06] = 0
	block[0x07] = 0

	// DxCountLimit at 0x08
	count := uint16(len(childBlocks))
	binary.LittleEndian.PutUint16(block[0x08:], 100) // limit
	binary.LittleEndian.PutUint16(block[0x0A:], count)

	// dx_entries starting at 0x0C
	if len(childBlocks) > 0 {
		binary.LittleEndian.PutUint32(block[0x0C:], childBlocks[0])
	}
	for i := 1; i < len(childBlocks); i++ {
		off := 0x10 + (i-1)*8
		binary.LittleEndian.PutUint32(block[off:], uint32(i*0x1000)) // hash
		binary.LittleEndian.PutUint32(block[off+4:], childBlocks[i]) // block
	}

	return block
}

func TestListEntriesHTreeDirect(t *testing.T) {
	const blockSize = 4096

	// Physical layout:
	// Block 4: HTree root (logical dir block 0)
	// Block 5: Leaf block (logical dir block 1) with entries
	// Block 6: Leaf block (logical dir block 2) with entries

	totalSize := 7 * blockSize
	image := make([]byte, totalSize)

	// HTree root at physical block 4 - points to leaf blocks 1 and 2
	rootBlock := buildHTreeRootBlock(blockSize, 0, []uint32{1, 2})
	copy(image[4*blockSize:], rootBlock)

	// Leaf block at physical block 5 (logical dir block 1)
	var leaf1Data []byte
	leaf1Data = append(leaf1Data, buildDirEntry(100, "hello.txt", 1)...)
	leaf1Data = append(leaf1Data, buildDirEntry(101, "world.txt", 1)...)
	copy(image[5*blockSize:], leaf1Data)

	// Leaf block at physical block 6 (logical dir block 2)
	var leaf2Data []byte
	leaf2Data = append(leaf2Data, buildDirEntry(102, "foo.txt", 1)...)
	copy(image[6*blockSize:], leaf2Data)

	// Build inode with extent tree: 3 logical blocks starting at physical block 4
	rootInode := &Inode{
		Mode:   0x4000 | 0755,
		Flags:  INDEX_FL | EXTENTS_FL,
		SizeLo: 3 * uint32(blockSize),
	}
	var extBuf bytes.Buffer
	binary.Write(&extBuf, binary.LittleEndian, &ExtentHeader{
		Magic: 0xF30A, Entries: 1, Max: 4, Depth: 0,
	})
	binary.Write(&extBuf, binary.LittleEndian, &Extent{
		Block: 0, Len: 3, StartHi: 0, StartLo: 4,
	})
	copy(rootInode.BlockOrExtents[:], extBuf.Bytes())

	// Construct FileSystem directly
	sr := io.NewSectionReader(bytes.NewReader(image), 0, int64(totalSize))
	ext4fs := &FileSystem{
		r:     sr,
		sb:    Superblock{LogBlockSize: 2}, // blockSize = 4096
		cache: &mockCache[string, any]{},
	}

	entries, err := ext4fs.listEntriesHTree(rootInode)
	if err != nil {
		t.Fatalf("listEntriesHTree failed: %v", err)
	}

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	names := map[string]bool{}
	for _, e := range entries {
		names[e.Name] = true
	}
	for _, expected := range []string{"hello.txt", "world.txt", "foo.txt"} {
		if !names[expected] {
			t.Errorf("expected %q in entries", expected)
		}
	}
}

func TestListEntriesHTreeWithInternalNodes(t *testing.T) {
	const blockSize = 4096

	// Physical layout:
	// Block 4: HTree root (logical dir block 0), indirect_levels=1
	//   -> points to internal nodes at logical blocks 1 and 2
	// Block 5: Internal node (logical dir block 1) -> leaf blocks 3, 4
	// Block 6: Internal node (logical dir block 2) -> leaf block 5
	// Block 7: Leaf block (logical dir block 3)
	// Block 8: Leaf block (logical dir block 4)
	// Block 9: Leaf block (logical dir block 5)

	totalSize := 10 * blockSize
	image := make([]byte, totalSize)

	// HTree root at physical block 4, indirect_levels=1
	rootBlock := buildHTreeRootBlock(blockSize, 1, []uint32{1, 2})
	copy(image[4*blockSize:], rootBlock)

	// Internal node at physical block 5 (logical block 1) -> leaf blocks 3 and 4
	internalNode1 := buildHTreeInternalNode(blockSize, []uint32{3, 4})
	copy(image[5*blockSize:], internalNode1)

	// Internal node at physical block 6 (logical block 2) -> leaf block 5
	internalNode2 := buildHTreeInternalNode(blockSize, []uint32{5})
	copy(image[6*blockSize:], internalNode2)

	// Leaf blocks
	var leaf3 []byte
	leaf3 = append(leaf3, buildDirEntry(10, "aaa", 1)...)
	copy(image[7*blockSize:], leaf3)

	var leaf4 []byte
	leaf4 = append(leaf4, buildDirEntry(11, "bbb", 1)...)
	copy(image[8*blockSize:], leaf4)

	var leaf5 []byte
	leaf5 = append(leaf5, buildDirEntry(12, "ccc", 1)...)
	copy(image[9*blockSize:], leaf5)

	// Build inode: 6 logical blocks starting at physical block 4
	rootInode := &Inode{
		Mode:   0x4000 | 0755,
		Flags:  INDEX_FL | EXTENTS_FL,
		SizeLo: 6 * uint32(blockSize),
	}
	var extBuf bytes.Buffer
	binary.Write(&extBuf, binary.LittleEndian, &ExtentHeader{
		Magic: 0xF30A, Entries: 1, Max: 4, Depth: 0,
	})
	binary.Write(&extBuf, binary.LittleEndian, &Extent{
		Block: 0, Len: 6, StartHi: 0, StartLo: 4,
	})
	copy(rootInode.BlockOrExtents[:], extBuf.Bytes())

	sr := io.NewSectionReader(bytes.NewReader(image), 0, int64(totalSize))
	ext4fs := &FileSystem{
		r:     sr,
		sb:    Superblock{LogBlockSize: 2},
		cache: &mockCache[string, any]{},
	}

	entries, err := ext4fs.listEntriesHTree(rootInode)
	if err != nil {
		t.Fatalf("listEntriesHTree failed: %v", err)
	}

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	names := map[string]bool{}
	for _, e := range entries {
		names[e.Name] = true
	}
	for _, expected := range []string{"aaa", "bbb", "ccc"} {
		if !names[expected] {
			t.Errorf("expected %q in entries", expected)
		}
	}
}

func TestListEntriesHTreeBlockAddressing(t *testing.T) {
	const blockSize = 4096

	// Physical layout (same as TestListEntriesHTreeDirect but using block addressing):
	// Block 4: HTree root (logical dir block 0)
	// Block 5: Leaf block (logical dir block 1)

	totalSize := 6 * blockSize
	image := make([]byte, totalSize)

	rootBlock := buildHTreeRootBlock(blockSize, 0, []uint32{1})
	copy(image[4*blockSize:], rootBlock)

	var leafData []byte
	leafData = append(leafData, buildDirEntry(100, "block_file", 1)...)
	copy(image[5*blockSize:], leafData)

	// Build inode with block addressing (no EXTENTS_FL)
	rootInode := &Inode{
		Mode:   0x4000 | 0755,
		Flags:  INDEX_FL, // no EXTENTS_FL
		SizeLo: 2 * uint32(blockSize),
	}
	// BlockAddressing: direct blocks [0]=4, [1]=5
	ba := BlockAddressing{}
	ba.DirectBlock[0] = 4
	ba.DirectBlock[1] = 5
	var baBuf bytes.Buffer
	binary.Write(&baBuf, binary.LittleEndian, &ba)
	copy(rootInode.BlockOrExtents[:], baBuf.Bytes())

	sr := io.NewSectionReader(bytes.NewReader(image), 0, int64(totalSize))
	ext4fs := &FileSystem{
		r:     sr,
		sb:    Superblock{LogBlockSize: 2},
		cache: &mockCache[string, any]{},
	}

	entries, err := ext4fs.listEntriesHTree(rootInode)
	if err != nil {
		t.Fatalf("listEntriesHTree failed: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Name != "block_file" {
		t.Errorf("expected 'block_file', got %q", entries[0].Name)
	}
}

func TestListEntriesHTreeRootBlockReadFailure(t *testing.T) {
	// Image too small: ReadAt will fail when trying to read the root block
	smallImage := make([]byte, 16)
	sr := io.NewSectionReader(bytes.NewReader(smallImage), 0, 16)
	ext4fs := &FileSystem{
		r:     sr,
		sb:    Superblock{LogBlockSize: 2}, // 4096 byte blocks
		cache: &mockCache[string, any]{},
	}

	rootInode := &Inode{
		Mode:   0x4000 | 0755,
		Flags:  INDEX_FL | EXTENTS_FL,
		SizeLo: uint32(4096),
	}
	var extBuf bytes.Buffer
	binary.Write(&extBuf, binary.LittleEndian, &ExtentHeader{
		Magic: 0xF30A, Entries: 1, Max: 4, Depth: 0,
	})
	binary.Write(&extBuf, binary.LittleEndian, &Extent{
		Block: 0, Len: 1, StartHi: 0, StartLo: 0,
	})
	copy(rootInode.BlockOrExtents[:], extBuf.Bytes())

	_, err := ext4fs.listEntriesHTree(rootInode)
	if err == nil {
		t.Fatal("expected error for root block read failure, got nil")
	}
}

func TestCollectLeafBlocksInternalNodeCountZero(t *testing.T) {
	const blockSize = 4096

	totalSize := 6 * blockSize
	image := make([]byte, totalSize)

	// Root block at physical block 4, indirect_levels=1, points to internal node at logical block 1
	rootBlock := buildHTreeRootBlock(blockSize, 1, []uint32{1})
	copy(image[4*blockSize:], rootBlock)

	// Internal node at physical block 5 (logical block 1): all zeros.
	// DxCountLimit.Count=0, so no child blocks are returned.

	sr := io.NewSectionReader(bytes.NewReader(image), 0, int64(totalSize))
	ext4fs := &FileSystem{
		r:     sr,
		sb:    Superblock{LogBlockSize: 2},
		cache: &mockCache[string, any]{},
	}

	rootInode := &Inode{
		Mode:   0x4000 | 0755,
		Flags:  INDEX_FL | EXTENTS_FL,
		SizeLo: 2 * uint32(blockSize),
	}
	var extBuf bytes.Buffer
	binary.Write(&extBuf, binary.LittleEndian, &ExtentHeader{
		Magic: 0xF30A, Entries: 1, Max: 4, Depth: 0,
	})
	binary.Write(&extBuf, binary.LittleEndian, &Extent{
		Block: 0, Len: 2, StartHi: 0, StartLo: 4,
	})
	copy(rootInode.BlockOrExtents[:], extBuf.Bytes())

	entries, err := ext4fs.listEntriesHTree(rootInode)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 entries from empty internal node, got %d", len(entries))
	}
}

func TestReadLogicalBlockMissingBlock(t *testing.T) {
	const blockSize = 4096

	image := make([]byte, blockSize)
	sr := io.NewSectionReader(bytes.NewReader(image), 0, int64(blockSize))
	ext4fs := &FileSystem{
		r:     sr,
		sb:    Superblock{LogBlockSize: 2},
		cache: &mockCache[string, any]{},
	}

	blockMap := map[uint32]int64{
		0: 0,
	}

	// Read existing block - should succeed
	_, err := ext4fs.readLogicalBlock(blockMap, 0)
	if err != nil {
		t.Fatalf("unexpected error reading existing block: %v", err)
	}

	// Read missing block - should fail
	_, err = ext4fs.readLogicalBlock(blockMap, 99)
	if err == nil {
		t.Fatal("expected error for missing logical block, got nil")
	}
}

func TestParseDxBlockNumbersTruncatedData(t *testing.T) {
	// count=3 but data only has room for header entry + 1 regular entry (not 2)
	data := make([]byte, 4+8)                    // block0(4) + entry1(8), missing entry2
	binary.LittleEndian.PutUint32(data[0:], 10)  // block0
	binary.LittleEndian.PutUint32(data[4:], 100) // hash1
	binary.LittleEndian.PutUint32(data[8:], 20)  // block1

	got := parseDxBlockNumbers(data, 3)
	// Should get 2 blocks (10, 20) since entry2 data is missing
	if len(got) != 2 {
		t.Fatalf("expected 2 blocks from truncated data, got %d", len(got))
	}
	if got[0] != 10 || got[1] != 20 {
		t.Errorf("expected [10, 20], got %v", got)
	}
}

func TestDxCountLimitParsing(t *testing.T) {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint16(data[0:], 200) // limit at offset 0
	binary.LittleEndian.PutUint16(data[2:], 5)   // count at offset 2

	var cl DxCountLimit
	if err := binary.Read(bytes.NewReader(data), binary.LittleEndian, &cl); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cl.Limit != 200 {
		t.Errorf("Limit = %d, want 200", cl.Limit)
	}
	if cl.Count != 5 {
		t.Errorf("Count = %d, want 5", cl.Count)
	}
}

func TestListEntriesHTreeCountExceedsLimit(t *testing.T) {
	const blockSize = 4096

	totalSize := 5 * blockSize
	image := make([]byte, totalSize)

	// Build root block with count > limit (corrupted)
	block := make([]byte, blockSize)
	// dot entry
	binary.LittleEndian.PutUint32(block[0x00:], 2)
	binary.LittleEndian.PutUint16(block[0x04:], 12)
	block[0x06] = 1
	block[0x07] = 2
	block[0x08] = '.'
	// dotdot entry
	binary.LittleEndian.PutUint32(block[0x0C:], 2)
	binary.LittleEndian.PutUint16(block[0x10:], uint16(blockSize-12))
	block[0x12] = 2
	block[0x13] = 2
	block[0x14] = '.'
	block[0x15] = '.'
	// dx_root_info
	block[0x1E] = 0 // indirect_levels
	// DxCountLimit: limit=2, count=10 (count > limit)
	binary.LittleEndian.PutUint16(block[0x20:], 2)
	binary.LittleEndian.PutUint16(block[0x22:], 10)

	copy(image[4*blockSize:], block)

	rootInode := &Inode{
		Mode:   0x4000 | 0755,
		Flags:  INDEX_FL | EXTENTS_FL,
		SizeLo: uint32(blockSize),
	}
	var extBuf bytes.Buffer
	binary.Write(&extBuf, binary.LittleEndian, &ExtentHeader{
		Magic: 0xF30A, Entries: 1, Max: 4, Depth: 0,
	})
	binary.Write(&extBuf, binary.LittleEndian, &Extent{
		Block: 0, Len: 1, StartHi: 0, StartLo: 4,
	})
	copy(rootInode.BlockOrExtents[:], extBuf.Bytes())

	sr := io.NewSectionReader(bytes.NewReader(image), 0, int64(totalSize))
	ext4fs := &FileSystem{
		r:     sr,
		sb:    Superblock{LogBlockSize: 2},
		cache: &mockCache[string, any]{},
	}

	_, err := ext4fs.listEntriesHTree(rootInode)
	if err == nil {
		t.Fatal("expected error for count > limit, got nil")
	}
}

func TestListEntriesHTreeInternalNodeCountExceedsLimit(t *testing.T) {
	const blockSize = 4096

	// Root (indirect_levels=1) -> internal node with count > limit
	totalSize := 6 * blockSize
	image := make([]byte, totalSize)

	rootBlock := buildHTreeRootBlock(blockSize, 1, []uint32{1})
	copy(image[4*blockSize:], rootBlock)

	// Internal node at physical block 5: limit=2, count=10 (corrupted)
	node := make([]byte, blockSize)
	binary.LittleEndian.PutUint32(node[0x00:], 0)
	binary.LittleEndian.PutUint16(node[0x04:], uint16(blockSize))
	binary.LittleEndian.PutUint16(node[0x08:], 2)  // limit
	binary.LittleEndian.PutUint16(node[0x0A:], 10) // count > limit
	copy(image[5*blockSize:], node)

	rootInode := &Inode{Mode: 0x4000 | 0755, Flags: INDEX_FL | EXTENTS_FL, SizeLo: 2 * uint32(blockSize)}
	var extBuf bytes.Buffer
	binary.Write(&extBuf, binary.LittleEndian, &ExtentHeader{Magic: 0xF30A, Entries: 1, Max: 4, Depth: 0})
	binary.Write(&extBuf, binary.LittleEndian, &Extent{Block: 0, Len: 2, StartHi: 0, StartLo: 4})
	copy(rootInode.BlockOrExtents[:], extBuf.Bytes())

	sr := io.NewSectionReader(bytes.NewReader(image), 0, int64(totalSize))
	ext4fs := &FileSystem{r: sr, sb: Superblock{LogBlockSize: 2}, cache: &mockCache[string, any]{}}

	_, err := ext4fs.listEntriesHTree(rootInode)
	if err == nil {
		t.Fatal("expected error for internal node count > limit, got nil")
	}
}

func TestListEntriesHTreeIndirectLevelsTooHigh(t *testing.T) {
	const blockSize = 4096

	// Without LARGEDIR: max is 2, so levels=3 should be rejected
	t.Run("without LARGEDIR", func(t *testing.T) {
		totalSize := 5 * blockSize
		image := make([]byte, totalSize)
		copy(image[4*blockSize:], buildHTreeRootBlock(blockSize, 3, []uint32{1}))

		rootInode := &Inode{Mode: 0x4000 | 0755, Flags: INDEX_FL | EXTENTS_FL, SizeLo: uint32(blockSize)}
		var extBuf bytes.Buffer
		binary.Write(&extBuf, binary.LittleEndian, &ExtentHeader{Magic: 0xF30A, Entries: 1, Max: 4, Depth: 0})
		binary.Write(&extBuf, binary.LittleEndian, &Extent{Block: 0, Len: 1, StartHi: 0, StartLo: 4})
		copy(rootInode.BlockOrExtents[:], extBuf.Bytes())

		sr := io.NewSectionReader(bytes.NewReader(image), 0, int64(totalSize))
		ext4fs := &FileSystem{r: sr, sb: Superblock{LogBlockSize: 2}, cache: &mockCache[string, any]{}}

		_, err := ext4fs.listEntriesHTree(rootInode)
		if err == nil {
			t.Fatal("expected error for indirect_levels=3 without LARGEDIR")
		}
	})

	// With LARGEDIR: max is 3, so levels=4 should be rejected
	t.Run("with LARGEDIR", func(t *testing.T) {
		totalSize := 5 * blockSize
		image := make([]byte, totalSize)
		copy(image[4*blockSize:], buildHTreeRootBlock(blockSize, 4, []uint32{1}))

		rootInode := &Inode{Mode: 0x4000 | 0755, Flags: INDEX_FL | EXTENTS_FL, SizeLo: uint32(blockSize)}
		var extBuf bytes.Buffer
		binary.Write(&extBuf, binary.LittleEndian, &ExtentHeader{Magic: 0xF30A, Entries: 1, Max: 4, Depth: 0})
		binary.Write(&extBuf, binary.LittleEndian, &Extent{Block: 0, Len: 1, StartHi: 0, StartLo: 4})
		copy(rootInode.BlockOrExtents[:], extBuf.Bytes())

		sr := io.NewSectionReader(bytes.NewReader(image), 0, int64(totalSize))
		sb := Superblock{LogBlockSize: 2, FeatureIncompat: FEATURE_INCOMPAT_LARGEDIR}
		ext4fs := &FileSystem{r: sr, sb: sb, cache: &mockCache[string, any]{}}

		_, err := ext4fs.listEntriesHTree(rootInode)
		if err == nil {
			t.Fatal("expected error for indirect_levels=4 with LARGEDIR")
		}
	})
}

func TestExtractDirectoryEntriesRecLenUnderflow(t *testing.T) {
	// Build an entry where RecLen < NameLen + 8 (corrupted)
	buf := make([]byte, 20)
	binary.LittleEndian.PutUint32(buf[0:], 1) // inode
	binary.LittleEndian.PutUint16(buf[4:], 5) // rec_len=5, but name_len=10 → 10+8=18 > 5
	buf[6] = 10                               // name_len=10
	buf[7] = 1                                // flags
	copy(buf[8:], []byte("abcdefghij"))       // name (10 bytes, extends beyond rec_len)

	entries, err := extractDirectoryEntries(bytes.NewBuffer(buf))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should stop parsing at corrupted entry, returning no entries
	if len(entries) != 0 {
		t.Errorf("expected 0 entries from corrupted RecLen, got %d", len(entries))
	}
}

func TestExtractDirectoryEntriesRecLenOverflow(t *testing.T) {
	// Build a valid entry followed by an entry whose RecLen exceeds remaining buffer.
	// The first entry should be returned; the second should cause parsing to stop.
	first := buildDirEntry(10, "hello", 1)            // valid entry
	second := make([]byte, 12)                        // entry with oversized RecLen
	binary.LittleEndian.PutUint32(second[0:], 20)     // inode=20
	binary.LittleEndian.PutUint16(second[4:], 0xFFFF) // rec_len=65535 (way beyond buffer)
	second[6] = 3                                     // name_len=3
	second[7] = 1                                     // flags
	copy(second[8:], []byte("foo"))

	buf := append(first, second...)
	entries, err := extractDirectoryEntries(bytes.NewBuffer(buf))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("expected 1 entry (first valid), got %d", len(entries))
	}
	if len(entries) > 0 && entries[0].Name != "hello" {
		t.Errorf("expected first entry name 'hello', got %q", entries[0].Name)
	}
}

func TestListEntriesDispatchesToHTree(t *testing.T) {
	const blockSize = 4096

	// Physical layout:
	// Block 2: inode table
	// Block 4: HTree root (logical dir block 0)
	// Block 5: Leaf block (logical dir block 1)

	totalSize := 6 * blockSize
	image := make([]byte, totalSize)

	rootBlock := buildHTreeRootBlock(blockSize, 0, []uint32{1})
	copy(image[4*blockSize:], rootBlock)

	var leafData []byte
	leafData = append(leafData, buildDirEntry(100, "dispatched", 1)...)
	copy(image[5*blockSize:], leafData)

	// Build root inode (inode 2) at inode table block 2, index 1
	rootInode := Inode{
		Mode:   0x4000 | 0755,
		Flags:  INDEX_FL | EXTENTS_FL,
		SizeLo: 2 * uint32(blockSize),
	}
	var extBuf bytes.Buffer
	binary.Write(&extBuf, binary.LittleEndian, &ExtentHeader{
		Magic: 0xF30A, Entries: 1, Max: 4, Depth: 0,
	})
	binary.Write(&extBuf, binary.LittleEndian, &Extent{
		Block: 0, Len: 2, StartHi: 0, StartLo: 4,
	})
	copy(rootInode.BlockOrExtents[:], extBuf.Bytes())

	// Write inode to inode table: block 2, index 1 (inode 2 = index 1)
	var inodeBuf bytes.Buffer
	binary.Write(&inodeBuf, binary.LittleEndian, &rootInode)
	copy(image[2*blockSize+256:], inodeBuf.Bytes())

	sr := io.NewSectionReader(bytes.NewReader(image), 0, int64(totalSize))
	ext4fs := &FileSystem{
		r:  sr,
		sb: Superblock{LogBlockSize: 2, InodePerGroup: 64, InodeSize: 256},
		gds: []GroupDescriptor{
			{GroupDescriptor32: GroupDescriptor32{InodeTableLo: 2}},
		},
		cache: &mockCache[string, any]{},
	}

	// Call listEntries (not listEntriesHTree) to verify dispatch
	entries, err := ext4fs.listEntries(rootInodeNumber)
	if err != nil {
		t.Fatalf("listEntries failed: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Name != "dispatched" {
		t.Errorf("expected 'dispatched', got %q", entries[0].Name)
	}
}

// --- GetBlockAddresses sparse tests ---

// buildBlockAddressingInode creates an Inode with block addressing (no EXTENTS_FL)
// and the given file size.
func buildBlockAddressingInode(directBlocks [12]uint32, singleIndirect, doubleIndirect, tripleIndirect uint32, fileSize int64) *Inode {
	inode := &Inode{
		Mode:     0x8000 | 0644,
		SizeLo:   uint32(fileSize),
		SizeHigh: uint32(fileSize >> 32),
	}
	ba := BlockAddressing{
		DirectBlock:         directBlocks,
		SingleIndirectBlock: singleIndirect,
		DoubleIndirectBlock: doubleIndirect,
		TripleIndirectBlock: tripleIndirect,
	}
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, &ba)
	copy(inode.BlockOrExtents[:], buf.Bytes())
	return inode
}

func TestGetBlockAddressesSparseDirectBlocks(t *testing.T) {
	const blockSize = 4096

	// Direct blocks: [5, 0, 7] — block 1 is a hole
	directBlocks := [12]uint32{5, 0, 7}
	inode := buildBlockAddressingInode(directBlocks, 0, 0, 0, 3*blockSize)

	image := make([]byte, 8*blockSize)
	sr := io.NewSectionReader(bytes.NewReader(image), 0, int64(len(image)))
	ext4fs := &FileSystem{r: sr, sb: Superblock{LogBlockSize: 2}, cache: &mockCache[string, any]{}}

	addrs, err := inode.GetBlockAddresses(ext4fs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(addrs) != 3 {
		t.Fatalf("expected 3 addresses, got %d", len(addrs))
	}
	if addrs[0] != 5 {
		t.Errorf("addrs[0] = %d, want 5", addrs[0])
	}
	if addrs[1] != 0 {
		t.Errorf("addrs[1] = %d, want 0 (hole)", addrs[1])
	}
	if addrs[2] != 7 {
		t.Errorf("addrs[2] = %d, want 7", addrs[2])
	}
}

func TestGetBlockAddressesSparseIndirectBlock(t *testing.T) {
	const blockSize = 4096

	// 12 direct blocks + 3 blocks via single indirect (with a hole at index 1)
	totalBlocks := int64(15)
	image := make([]byte, 20*blockSize)

	// Single indirect block at physical block 16
	// Contains: [100, 0, 200] — middle entry is a hole
	indirectBlock := make([]byte, blockSize)
	binary.LittleEndian.PutUint32(indirectBlock[0:], 100)
	binary.LittleEndian.PutUint32(indirectBlock[4:], 0) // hole
	binary.LittleEndian.PutUint32(indirectBlock[8:], 200)
	copy(image[16*blockSize:], indirectBlock)

	directBlocks := [12]uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	inode := buildBlockAddressingInode(directBlocks, 16, 0, 0, totalBlocks*blockSize)

	sr := io.NewSectionReader(bytes.NewReader(image), 0, int64(len(image)))
	ext4fs := &FileSystem{r: sr, sb: Superblock{LogBlockSize: 2}, cache: &mockCache[string, any]{}}

	addrs, err := inode.GetBlockAddresses(ext4fs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if int64(len(addrs)) != totalBlocks {
		t.Fatalf("expected %d addresses, got %d", totalBlocks, len(addrs))
	}

	// Check direct blocks [0-11]
	for i := 0; i < 12; i++ {
		if addrs[i] != uint32(i+1) {
			t.Errorf("addrs[%d] = %d, want %d", i, addrs[i], i+1)
		}
	}
	// Check indirect blocks [12-14]
	if addrs[12] != 100 {
		t.Errorf("addrs[12] = %d, want 100", addrs[12])
	}
	if addrs[13] != 0 {
		t.Errorf("addrs[13] = %d, want 0 (hole)", addrs[13])
	}
	if addrs[14] != 200 {
		t.Errorf("addrs[14] = %d, want 200", addrs[14])
	}
}

func TestGetBlockAddressesNullIndirectPointers(t *testing.T) {
	const blockSize = 4096

	// 14 blocks total: 12 direct + 2 more needed, but SingleIndirectBlock=0 (null pointer).
	// The null pointer path should emit zeros for the remaining 2 blocks.
	totalBlocks := int64(14)
	directBlocks := [12]uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	inode := buildBlockAddressingInode(directBlocks, 0, 0, 0, totalBlocks*blockSize)

	image := make([]byte, blockSize)
	sr := io.NewSectionReader(bytes.NewReader(image), 0, int64(len(image)))
	ext4fs := &FileSystem{r: sr, sb: Superblock{LogBlockSize: 2}, cache: &mockCache[string, any]{}}

	addrs, err := inode.GetBlockAddresses(ext4fs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if int64(len(addrs)) != totalBlocks {
		t.Fatalf("expected %d addresses, got %d", totalBlocks, len(addrs))
	}

	// Direct blocks [0-11] should have real addresses
	for i := 0; i < 12; i++ {
		if addrs[i] != uint32(i+1) {
			t.Errorf("addrs[%d] = %d, want %d", i, addrs[i], i+1)
		}
	}
	// Blocks [12-13] should be zero (null indirect pointer)
	if addrs[12] != 0 {
		t.Errorf("addrs[12] = %d, want 0 (null indirect pointer)", addrs[12])
	}
	if addrs[13] != 0 {
		t.Errorf("addrs[13] = %d, want 0 (null indirect pointer)", addrs[13])
	}
}

// --- buildDirectoryBlockMap zero-skip test ---

func TestBuildDirectoryBlockMapSkipsZero(t *testing.T) {
	const blockSize = 4096

	// Direct blocks: [4, 0, 6] — block 1 is a hole
	directBlocks := [12]uint32{4, 0, 6}
	inode := buildBlockAddressingInode(directBlocks, 0, 0, 0, 3*blockSize)
	// Override mode to directory
	inode.Mode = 0x4000 | 0755

	image := make([]byte, 8*blockSize)
	sr := io.NewSectionReader(bytes.NewReader(image), 0, int64(len(image)))
	ext4fs := &FileSystem{r: sr, sb: Superblock{LogBlockSize: 2}, cache: &mockCache[string, any]{}}

	m, err := ext4fs.buildDirectoryBlockMap(inode)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Block 0 -> offset 4*4096
	if offset, ok := m[0]; !ok || offset != 4*blockSize {
		t.Errorf("block 0: got offset=%d ok=%v, want %d", offset, ok, 4*blockSize)
	}
	// Block 1 should NOT be in the map (hole)
	if _, ok := m[1]; ok {
		t.Errorf("block 1 should not be in map (hole)")
	}
	// Block 2 -> offset 6*4096
	if offset, ok := m[2]; !ok || offset != 6*blockSize {
		t.Errorf("block 2: got offset=%d ok=%v, want %d", offset, ok, 6*blockSize)
	}
}

func TestBuildDirectoryBlockMapExtents(t *testing.T) {
	const blockSize = 4096

	// Inode with EXTENTS_FL: two extents
	// Extent 1: logical blocks 0-1 at physical blocks 10-11
	// Extent 2: logical block 2 at physical block 20
	inode := &Inode{
		Mode:   0x4000 | 0755,
		Flags:  EXTENTS_FL,
		SizeLo: 3 * uint32(blockSize),
	}
	var extBuf bytes.Buffer
	binary.Write(&extBuf, binary.LittleEndian, &ExtentHeader{
		Magic: 0xF30A, Entries: 2, Max: 4, Depth: 0,
	})
	binary.Write(&extBuf, binary.LittleEndian, &Extent{
		Block: 0, Len: 2, StartHi: 0, StartLo: 10,
	})
	binary.Write(&extBuf, binary.LittleEndian, &Extent{
		Block: 2, Len: 1, StartHi: 0, StartLo: 20,
	})
	copy(inode.BlockOrExtents[:], extBuf.Bytes())

	image := make([]byte, 25*blockSize)
	sr := io.NewSectionReader(bytes.NewReader(image), 0, int64(len(image)))
	ext4fs := &FileSystem{r: sr, sb: Superblock{LogBlockSize: 2}, cache: &mockCache[string, any]{}}

	m, err := ext4fs.buildDirectoryBlockMap(inode)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Logical block 0 -> physical block 10 -> offset 10*4096
	if offset, ok := m[0]; !ok || offset != 10*blockSize {
		t.Errorf("block 0: got offset=%d ok=%v, want %d", offset, ok, 10*blockSize)
	}
	// Logical block 1 -> physical block 11 -> offset 11*4096
	if offset, ok := m[1]; !ok || offset != 11*blockSize {
		t.Errorf("block 1: got offset=%d ok=%v, want %d", offset, ok, 11*blockSize)
	}
	// Logical block 2 -> physical block 20 -> offset 20*4096
	if offset, ok := m[2]; !ok || offset != 20*blockSize {
		t.Errorf("block 2: got offset=%d ok=%v, want %d", offset, ok, 20*blockSize)
	}

	if len(m) != 3 {
		t.Errorf("expected 3 entries in block map, got %d", len(m))
	}
}

func TestBuildDirectoryBlockMapRejectsUninitializedExtent(t *testing.T) {
	const blockSize = 4096

	inode := &Inode{
		Mode:   0x4000 | 0755,
		Flags:  EXTENTS_FL,
		SizeLo: 2 * uint32(blockSize),
	}
	var extBuf bytes.Buffer
	binary.Write(&extBuf, binary.LittleEndian, &ExtentHeader{
		Magic: 0xF30A, Entries: 2, Max: 4, Depth: 0,
	})
	// Extent 1: initialized
	binary.Write(&extBuf, binary.LittleEndian, &Extent{
		Block: 0, Len: 1, StartHi: 0, StartLo: 10,
	})
	// Extent 2: uninitialized (bit 15 set)
	binary.Write(&extBuf, binary.LittleEndian, &Extent{
		Block: 1, Len: 0x8001, StartHi: 0, StartLo: 20,
	})
	copy(inode.BlockOrExtents[:], extBuf.Bytes())

	image := make([]byte, 25*blockSize)
	sr := io.NewSectionReader(bytes.NewReader(image), 0, int64(len(image)))
	ext4fs := &FileSystem{r: sr, sb: Superblock{LogBlockSize: 2}, cache: &mockCache[string, any]{}}

	_, err := ext4fs.buildDirectoryBlockMap(inode)
	if err == nil {
		t.Fatal("expected error for uninitialized extent in directory, got nil")
	}
	if !strings.Contains(err.Error(), "failed to build directory block map: uninitialized extent") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// --- fileFromBlock zero-skip test ---

func TestFileFromBlockSkipsZeroAddresses(t *testing.T) {
	const blockSize = 4096

	// 3 blocks: [10, 0, 12] — block 1 is a hole
	directBlocks := [12]uint32{10, 0, 12}
	inode := buildBlockAddressingInode(directBlocks, 0, 0, 0, 3*blockSize)

	image := make([]byte, 16*blockSize)

	// Write known data to block 10 and 12
	for i := range image[10*blockSize : 11*blockSize] {
		image[10*blockSize+i] = 0xAA
	}
	for i := range image[12*blockSize : 13*blockSize] {
		image[12*blockSize+i] = 0xCC
	}

	sr := io.NewSectionReader(bytes.NewReader(image), 0, int64(len(image)))
	ext4fs := &FileSystem{r: sr, sb: Superblock{LogBlockSize: 2}, cache: &mockCache[string, any]{}}

	fi := FileInfo{
		name:  "sparse.bin",
		inode: inode,
	}
	f, err := ext4fs.fileFromBlock(fi, "sparse.bin")
	if err != nil {
		t.Fatalf("fileFromBlock failed: %v", err)
	}

	// Block 0 (physical 10) should be in table
	if _, ok := f.table[0]; !ok {
		t.Error("block 0 should be in data table")
	}
	// Block 1 (hole) should NOT be in table
	if _, ok := f.table[1]; ok {
		t.Error("block 1 (hole) should not be in data table")
	}
	// Block 2 (physical 12) should be in table
	if _, ok := f.table[2]; !ok {
		t.Error("block 2 should be in data table")
	}

	// Read block 0 — should be 0xAA
	buf := make([]byte, blockSize)
	n, err := f.Read(buf)
	if err != nil {
		t.Fatalf("Read block 0 failed: %v", err)
	}
	if n != blockSize {
		t.Fatalf("Read block 0: got %d bytes, want %d", n, blockSize)
	}
	if buf[0] != 0xAA || buf[blockSize-1] != 0xAA {
		t.Errorf("block 0 data: got %#x...%#x, want 0xAA", buf[0], buf[blockSize-1])
	}

	// Read block 1 — hole, should be zeros
	n, err = f.Read(buf)
	if err != nil {
		t.Fatalf("Read block 1 (hole) failed: %v", err)
	}
	for i, b := range buf[:n] {
		if b != 0 {
			t.Errorf("block 1 (hole) byte[%d] = %#x, want 0", i, b)
			break
		}
	}

	// Read block 2 — should be 0xCC
	n, err = f.Read(buf)
	if err != nil {
		t.Fatalf("Read block 2 failed: %v", err)
	}
	if buf[0] != 0xCC || buf[n-1] != 0xCC {
		t.Errorf("block 2 data: got %#x...%#x, want 0xCC", buf[0], buf[n-1])
	}
}

// --- File.Read partial last block test ---

func TestFileReadPartialLastBlock(t *testing.T) {
	const blockSize = 4096
	// File size = 1.5 blocks = 6144 bytes
	fileSize := int64(blockSize + blockSize/2)

	image := make([]byte, 16*blockSize)

	// Write known data to physical block 10 (full block: 0xAA)
	for i := 0; i < blockSize; i++ {
		image[10*blockSize+i] = 0xAA
	}
	// Write known data to physical block 11 (full block: 0xBB, but only half should be read)
	for i := 0; i < blockSize; i++ {
		image[11*blockSize+i] = 0xBB
	}

	directBlocks := [12]uint32{10, 11}
	inode := buildBlockAddressingInode(directBlocks, 0, 0, 0, fileSize)

	sr := io.NewSectionReader(bytes.NewReader(image), 0, int64(len(image)))
	ext4fs := &FileSystem{r: sr, sb: Superblock{LogBlockSize: 2}, cache: &mockCache[string, any]{}}

	fi := FileInfo{
		name:  "partial.bin",
		inode: inode,
	}
	f, err := ext4fs.fileFromBlock(fi, "partial.bin")
	if err != nil {
		t.Fatalf("fileFromBlock failed: %v", err)
	}

	// Read entire file
	var all []byte
	buf := make([]byte, 1024)
	for {
		n, err := f.Read(buf)
		if n > 0 {
			all = append(all, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
	}

	if int64(len(all)) != fileSize {
		t.Fatalf("total bytes read = %d, want %d", len(all), fileSize)
	}

	// First block should be 0xAA
	for i := 0; i < blockSize; i++ {
		if all[i] != 0xAA {
			t.Errorf("byte[%d] = %#x, want 0xAA", i, all[i])
			break
		}
	}

	// Second block (partial) should be 0xBB for exactly blockSize/2 bytes
	for i := blockSize; i < int(fileSize); i++ {
		if all[i] != 0xBB {
			t.Errorf("byte[%d] = %#x, want 0xBB", i, all[i])
			break
		}
	}
}

// --- listEntries non-HTree extent path ---

func TestListEntriesExtentReadAt(t *testing.T) {
	const blockSize = 4096

	// Physical layout:
	// Block 2: inode table
	// Block 4: directory block (logical block 0) with entries
	// Block 5: directory block (logical block 1) with entries

	totalSize := 6 * blockSize
	image := make([]byte, totalSize)

	// Directory entries at physical block 4
	// In real ext4, the last entry in a block has RecLen padded to fill the block.
	var block0Data []byte
	block0Data = append(block0Data, buildDirEntry(2, ".", 2)...)
	block0Data = append(block0Data, buildDirEntry(2, "..", 2)...)
	extAEntry := buildDirEntry(100, "extA", 1)
	// Pad last entry's RecLen to fill the rest of block 4
	binary.LittleEndian.PutUint16(extAEntry[4:6], uint16(blockSize-len(block0Data)))
	block0Data = append(block0Data, extAEntry...)
	copy(image[4*blockSize:], block0Data)

	// Directory entries at physical block 5
	var block1Data []byte
	block1Data = append(block1Data, buildDirEntry(101, "extB", 1)...)
	copy(image[5*blockSize:], block1Data)

	// Build root inode (inode 2) — directory with EXTENTS_FL, no INDEX_FL
	rootInode := Inode{
		Mode:   0x4000 | 0755,
		Flags:  EXTENTS_FL, // extent-based, no HTree
		SizeLo: 2 * uint32(blockSize),
	}
	var extBuf bytes.Buffer
	binary.Write(&extBuf, binary.LittleEndian, &ExtentHeader{
		Magic: 0xF30A, Entries: 1, Max: 4, Depth: 0,
	})
	binary.Write(&extBuf, binary.LittleEndian, &Extent{
		Block: 0, Len: 2, StartHi: 0, StartLo: 4,
	})
	copy(rootInode.BlockOrExtents[:], extBuf.Bytes())

	// Write inode to inode table: block 2, index 1 (inode 2 = index 1)
	var inodeBuf bytes.Buffer
	binary.Write(&inodeBuf, binary.LittleEndian, &rootInode)
	copy(image[2*blockSize+256:], inodeBuf.Bytes())

	sr := io.NewSectionReader(bytes.NewReader(image), 0, int64(totalSize))
	ext4fs := &FileSystem{
		r:  sr,
		sb: Superblock{LogBlockSize: 2, InodePerGroup: 64, InodeSize: 256},
		gds: []GroupDescriptor{
			{GroupDescriptor32: GroupDescriptor32{InodeTableLo: 2}},
		},
		cache: &mockCache[string, any]{},
	}

	entries, err := ext4fs.listEntries(rootInodeNumber)
	if err != nil {
		t.Fatalf("listEntries failed: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	names := map[string]bool{}
	for _, e := range entries {
		names[e.Name] = true
	}
	for _, expected := range []string{"extA", "extB"} {
		if !names[expected] {
			t.Errorf("expected %q in entries", expected)
		}
	}
}

func TestListEntriesExtentRejectsUninitializedExtent(t *testing.T) {
	const blockSize = 4096

	totalSize := 6 * blockSize
	image := make([]byte, totalSize)

	// Build root inode (inode 2) with an uninitialized extent
	rootInode := Inode{
		Mode:   0x4000 | 0755,
		Flags:  EXTENTS_FL,
		SizeLo: uint32(blockSize),
	}
	var extBuf bytes.Buffer
	binary.Write(&extBuf, binary.LittleEndian, &ExtentHeader{
		Magic: 0xF30A, Entries: 1, Max: 4, Depth: 0,
	})
	binary.Write(&extBuf, binary.LittleEndian, &Extent{
		Block: 0, Len: 0x8001, StartHi: 0, StartLo: 4, // uninitialized
	})
	copy(rootInode.BlockOrExtents[:], extBuf.Bytes())

	var inodeBuf bytes.Buffer
	binary.Write(&inodeBuf, binary.LittleEndian, &rootInode)
	copy(image[2*blockSize+256:], inodeBuf.Bytes())

	sr := io.NewSectionReader(bytes.NewReader(image), 0, int64(totalSize))
	ext4fs := &FileSystem{
		r:  sr,
		sb: Superblock{LogBlockSize: 2, InodePerGroup: 64, InodeSize: 256},
		gds: []GroupDescriptor{
			{GroupDescriptor32: GroupDescriptor32{InodeTableLo: 2}},
		},
		cache: &mockCache[string, any]{},
	}

	_, err := ext4fs.listEntries(rootInodeNumber)
	if err == nil {
		t.Fatal("expected error for uninitialized extent in directory, got nil")
	}
	if !strings.Contains(err.Error(), "failed to list directory entries: uninitialized extent") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// --- listEntries non-HTree block addressing path ---

func TestListEntriesBlockAddressingReadAt(t *testing.T) {
	const blockSize = 4096

	// Physical layout:
	// Block 2: inode table
	// Block 4: directory block (logical block 0) with entries
	// Block 5: directory block (logical block 1) with entries

	totalSize := 6 * blockSize
	image := make([]byte, totalSize)

	// Directory entries at physical block 4
	var block0Data []byte
	block0Data = append(block0Data, buildDirEntry(2, ".", 2)...)
	block0Data = append(block0Data, buildDirEntry(2, "..", 2)...)
	block0Data = append(block0Data, buildDirEntry(100, "fileA", 1)...)
	copy(image[4*blockSize:], block0Data)

	// Directory entries at physical block 5
	var block1Data []byte
	block1Data = append(block1Data, buildDirEntry(101, "fileB", 1)...)
	copy(image[5*blockSize:], block1Data)

	// Build root inode (inode 2) — directory with block addressing, no EXTENTS_FL, no INDEX_FL
	rootInode := Inode{
		Mode:   0x4000 | 0755,
		Flags:  0, // no EXTENTS_FL, no INDEX_FL
		SizeLo: 2 * uint32(blockSize),
	}
	ba := BlockAddressing{}
	ba.DirectBlock[0] = 4
	ba.DirectBlock[1] = 5
	var baBuf bytes.Buffer
	binary.Write(&baBuf, binary.LittleEndian, &ba)
	copy(rootInode.BlockOrExtents[:], baBuf.Bytes())

	// Write inode to inode table: block 2, index 1 (inode 2 = index 1)
	var inodeBuf bytes.Buffer
	binary.Write(&inodeBuf, binary.LittleEndian, &rootInode)
	copy(image[2*blockSize+256:], inodeBuf.Bytes())

	sr := io.NewSectionReader(bytes.NewReader(image), 0, int64(totalSize))
	ext4fs := &FileSystem{
		r:  sr,
		sb: Superblock{LogBlockSize: 2, InodePerGroup: 64, InodeSize: 256},
		gds: []GroupDescriptor{
			{GroupDescriptor32: GroupDescriptor32{InodeTableLo: 2}},
		},
		cache: &mockCache[string, any]{},
	}

	entries, err := ext4fs.listEntries(rootInodeNumber)
	if err != nil {
		t.Fatalf("listEntries failed: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	names := map[string]bool{}
	for _, e := range entries {
		names[e.Name] = true
	}
	for _, expected := range []string{"fileA", "fileB"} {
		if !names[expected] {
			t.Errorf("expected %q in entries", expected)
		}
	}
}
