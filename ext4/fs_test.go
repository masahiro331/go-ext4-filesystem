package ext4

import (
	"bytes"
	"encoding/binary"
	"io"
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

func TestExtractDirectoryEntriesSkipsChecksum(t *testing.T) {
	var data []byte
	data = append(data, buildDirEntry(5, "keep", 1)...)
	data = append(data, buildDirEntry(6, "csum", 0xDE)...)

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
