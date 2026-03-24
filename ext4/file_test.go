package ext4

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/fs"
	"testing"
)

// newTestFile creates a File with the given parameters for testing Read().
// The dataTable maps logical block numbers to byte offsets in the image.
func newTestFile(image []byte, blockSize int64, fileSize int64, table dataTable) *File {
	r := io.NewSectionReader(bytes.NewReader(image), 0, int64(len(image)))
	fs := &FileSystem{r: r}
	inode := &Inode{SizeLo: uint32(fileSize)}

	return &File{
		FileInfo: FileInfo{
			name:  "test",
			inode: inode,
		},
		fs:           fs,
		currentBlock: -1,
		buffer:       bytes.NewBuffer(nil),
		blockSize:    blockSize,
		table:        table,
		size:         fileSize,
	}
}

func readAll(t *testing.T, f *File) []byte {
	t.Helper()
	var result []byte
	buf := make([]byte, 256)
	for {
		n, err := f.Read(buf)
		if n > 0 {
			result = append(result, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read error: %v", err)
		}
	}
	return result
}

func TestFileRead_SparseLastBlock(t *testing.T) {
	// File with blockSize=512, size=2000 (4 blocks: 3 full + 1 partial of 464 bytes).
	// Block 0-2 have data, block 3 is sparse (not in table).
	const blockSize = 512
	const fileSize = 2000

	// Build image with 3 blocks of known data
	image := make([]byte, blockSize*3)
	for i := 0; i < 3; i++ {
		for j := 0; j < blockSize; j++ {
			image[i*blockSize+j] = byte(i + 1) // block 0: 0x01, block 1: 0x02, block 2: 0x03
		}
	}

	table := dataTable{
		0: 0,
		1: blockSize,
		2: blockSize * 2,
		// block 3 is sparse (missing from table)
	}

	f := newTestFile(image, blockSize, fileSize, table)
	data := readAll(t, f)

	if int64(len(data)) != fileSize {
		t.Fatalf("read %d bytes, want %d", len(data), fileSize)
	}

	// Verify first 3 blocks have correct data
	for i := 0; i < 3; i++ {
		for j := 0; j < blockSize; j++ {
			if data[i*blockSize+j] != byte(i+1) {
				t.Fatalf("block %d byte %d: got %#x, want %#x", i, j, data[i*blockSize+j], byte(i+1))
			}
		}
	}

	// Verify last partial block (464 bytes) is all zeros
	sparseStart := 3 * blockSize
	remaining := fileSize - sparseStart
	for j := 0; j < remaining; j++ {
		if data[sparseStart+j] != 0 {
			t.Fatalf("sparse block byte %d: got %#x, want 0", j, data[sparseStart+j])
		}
	}
}

func TestFileRead_SparseFullBlock(t *testing.T) {
	// File with blockSize=512, size=1024. Block 0 has data, block 1 is sparse.
	const blockSize = 512
	const fileSize = 1024

	image := make([]byte, blockSize)
	for j := 0; j < blockSize; j++ {
		image[j] = 0xAA
	}

	table := dataTable{
		0: 0,
		// block 1 is sparse
	}

	f := newTestFile(image, blockSize, fileSize, table)
	data := readAll(t, f)

	if int64(len(data)) != fileSize {
		t.Fatalf("read %d bytes, want %d", len(data), fileSize)
	}

	// Block 0: 0xAA
	for j := 0; j < blockSize; j++ {
		if data[j] != 0xAA {
			t.Fatalf("block 0 byte %d: got %#x, want 0xAA", j, data[j])
		}
	}

	// Block 1: all zeros
	for j := blockSize; j < fileSize; j++ {
		if data[j] != 0 {
			t.Fatalf("sparse block byte %d: got %#x, want 0", j-blockSize, data[j])
		}
	}
}

func TestFileRead_AllSparse(t *testing.T) {
	// Entirely sparse file: blockSize=512, size=768 (1 full + 1 partial).
	const blockSize = 512
	const fileSize = 768

	image := make([]byte, 0) // no data blocks
	table := dataTable{}     // everything sparse

	f := newTestFile(image, blockSize, fileSize, table)
	data := readAll(t, f)

	if int64(len(data)) != fileSize {
		t.Fatalf("read %d bytes, want %d", len(data), fileSize)
	}

	for i, b := range data {
		if b != 0 {
			t.Fatalf("byte %d: got %#x, want 0", i, b)
		}
	}
}

func TestFileInfoMode(t *testing.T) {
	tests := []struct {
		name     string
		imode    uint16
		wantType fs.FileMode
		wantPerm fs.FileMode
	}{
		{"regular 0644", 0x81A4, 0, 0o644},
		{"directory 0755", 0x41ED, fs.ModeDir, 0o755},
		{"symlink 0777", 0xA1FF, fs.ModeSymlink, 0o777},
		{"socket 0755", 0xC1ED, fs.ModeSocket, 0o755},
		{"fifo 0644", 0x11A4, fs.ModeNamedPipe, 0o644},
		{"char device 0666", 0x21B6, fs.ModeCharDevice | fs.ModeDevice, 0o666},
		{"block device 0660", 0x61B0, fs.ModeDevice, 0o660},
		{"setuid", 0x89A4, 0, 0o644 | fs.ModeSetuid},
		{"setgid", 0x85A4, 0, 0o644 | fs.ModeSetgid},
		{"sticky", 0x83A4, 0, 0o644 | fs.ModeSticky},
		{"setuid+setgid+sticky", 0x8FA4, 0, 0o644 | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fi := FileInfo{
				name:  "test",
				inode: &Inode{Mode: tt.imode},
			}
			got := fi.Mode()

			if got.Type() != tt.wantType {
				t.Errorf("Type() = %v, want %v", got.Type(), tt.wantType)
			}
			if got.Perm() != tt.wantPerm.Perm() {
				t.Errorf("Perm() = %o, want %o", got.Perm(), tt.wantPerm.Perm())
			}
			if tt.wantPerm&fs.ModeSetuid != 0 && got&fs.ModeSetuid == 0 {
				t.Error("ModeSetuid not set")
			}
			if tt.wantPerm&fs.ModeSetgid != 0 && got&fs.ModeSetgid == 0 {
				t.Error("ModeSetgid not set")
			}
			if tt.wantPerm&fs.ModeSticky != 0 && got&fs.ModeSticky == 0 {
				t.Error("ModeSticky not set")
			}

			// Verify Go standard API works correctly
			switch tt.wantType {
			case fs.ModeDir:
				if !got.IsDir() {
					t.Error("IsDir() = false, want true")
				}
				if got.IsRegular() {
					t.Error("IsRegular() = true, want false")
				}
			case 0:
				if !got.IsRegular() {
					t.Error("IsRegular() = false, want true")
				}
				if got.IsDir() {
					t.Error("IsDir() = true, want false")
				}
			default:
				if got.IsDir() {
					t.Error("IsDir() should be false for non-dir type")
				}
				if got.IsRegular() {
					t.Error("IsRegular() should be false for non-regular type")
				}
			}
		})
	}
}

// TestFileRead_UninitializedExtentReadsZeros verifies that file() skips
// uninitialized extents, and Read() returns zeros for those blocks via the
// sparse path.
func TestFileRead_UninitializedExtentReadsZeros(t *testing.T) {
	const blockSize = 4096
	// 4 blocks total: block 0-1 initialized, block 2-3 uninitialized
	const fileSize = blockSize * 4

	// Build image: 4 blocks of disk data.
	// Blocks at physical position 0-1 contain 0xAA (will be mapped to logical 0-1).
	// Blocks at physical position 2-3 contain 0xBB (uninitialized extent points here,
	// but these should NOT be read).
	image := make([]byte, blockSize*4)
	for i := 0; i < blockSize*2; i++ {
		image[i] = 0xAA
	}
	for i := blockSize * 2; i < blockSize*4; i++ {
		image[i] = 0xBB
	}

	r := io.NewSectionReader(bytes.NewReader(image), 0, int64(len(image)))

	// Build inode with extent tree in BlockOrExtents:
	// ExtentHeader (depth=0, 2 entries) + 2 Extents
	var extData [60]byte
	buf := bytes.NewBuffer(extData[:0])
	binary.Write(buf, binary.LittleEndian, ExtentHeader{
		Magic: 0xF30A, Entries: 2, Max: 4, Depth: 0,
	})
	// Extent 1: blocks 0-1, initialized, physical block 0
	binary.Write(buf, binary.LittleEndian, Extent{
		Block: 0, Len: 2, StartHi: 0, StartLo: 0,
	})
	// Extent 2: blocks 2-3, uninitialized (bit 15 set), physical block 2
	binary.Write(buf, binary.LittleEndian, Extent{
		Block: 2, Len: 0x8002, StartHi: 0, StartLo: 2,
	})
	copy(extData[:], buf.Bytes())

	inode := &Inode{
		SizeLo: fileSize,
		Flags:  EXTENTS_FL,
	}
	copy(inode.BlockOrExtents[:], extData[:])

	fs := &FileSystem{
		r:  r,
		sb: Superblock{LogBlockSize: 2}, // 4096
	}

	fi := FileInfo{name: "test", inode: inode}
	f, err := fs.file(fi, "test")
	if err != nil {
		t.Fatalf("file() error: %v", err)
	}

	data := readAll(t, f)
	if int64(len(data)) != fileSize {
		t.Fatalf("read %d bytes, want %d", len(data), fileSize)
	}

	// Blocks 0-1: should be 0xAA (initialized extent)
	for i := 0; i < blockSize*2; i++ {
		if data[i] != 0xAA {
			t.Fatalf("byte %d (initialized): got %#x, want 0xAA", i, data[i])
		}
	}

	// Blocks 2-3: should be 0x00 (uninitialized extent → zeros, NOT 0xBB)
	for i := blockSize * 2; i < fileSize; i++ {
		if data[i] != 0 {
			t.Fatalf("byte %d (uninitialized): got %#x, want 0x00", i, data[i])
		}
	}
}
