package ext4

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

// newTestFile creates a File with the given parameters for testing Read().
// The dataTable maps logical block numbers to byte offsets in the image.
func newTestFile(image []byte, blockSize int64, fileSize int64, table dataTable) *File {
	r := io.NewSectionReader(bytes.NewReader(image), 0, int64(len(image)))
	fs := &FileSystem{r: r}
	inode := &Inode{}
	binary.LittleEndian.PutUint32(
		(*[4]byte)(inode.BlockOrExtents[40:44])[:],
		uint32(fileSize),
	)
	// Set SizeLo directly
	inode.SizeLo = uint32(fileSize)

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
