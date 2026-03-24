package ext4

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

func TestGetGroupDescriptorTableCount(t *testing.T) {
	tests := []struct {
		name            string
		blockCountLo    uint32
		blockCountHi    uint32
		blockPerGroup   uint32
		firstDataBlock  uint32
		featureIncompat uint32 // set FEATURE_INCOMPAT_64BIT if needed
		want            uint32
	}{
		{
			name:          "128MB 4KB blocks: exactly 1 group",
			blockCountLo:  32768, // 128MB / 4KB
			blockPerGroup: 32768,
			want:          1,
		},
		{
			name:          "128MB+1block 4KB blocks: 2 groups",
			blockCountLo:  32769,
			blockPerGroup: 32768,
			want:          2,
		},
		{
			name:          "256MB 4KB blocks: exactly 2 groups",
			blockCountLo:  65536, // 256MB / 4KB
			blockPerGroup: 32768,
			want:          2,
		},
		{
			name:           "1KB block size: firstDataBlock=1",
			blockCountLo:   131072, // 128MB / 1KB
			blockPerGroup:  8192,
			firstDataBlock: 1,
			want:           16, // (131072-1+8191)/8192 = 16
		},
		{
			name:           "1KB block size: exact division",
			blockCountLo:   8193, // 8192 usable blocks + 1 first_data_block
			blockPerGroup:  8192,
			firstDataBlock: 1,
			want:           1,
		},
		{
			name:            "64bit mode: BlockCountHi contributes",
			blockCountLo:    0,
			blockCountHi:    1, // total = 1<<32 = 4294967296 blocks
			blockPerGroup:   32768,
			featureIncompat: FEATURE_INCOMPAT_64BIT,
			want:            131072, // 4294967296 / 32768
		},
		{
			name:          "small FS: fewer blocks than one group",
			blockCountLo:  1024,
			blockPerGroup: 32768,
			want:          1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sb := &Superblock{
				BlockCountLo:    tt.blockCountLo,
				BlockCountHi:    tt.blockCountHi,
				BlockPerGroup:   tt.blockPerGroup,
				FirstDataBlock:  tt.firstDataBlock,
				FeatureIncompat: tt.featureIncompat,
			}
			got := sb.GetGroupDescriptorTableCount()
			if got != tt.want {
				t.Errorf("GetGroupDescriptorTableCount() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestGetGroupDescriptorTableCount_32bitIgnoresHi(t *testing.T) {
	// In 32-bit mode, BlockCountHi must NOT be used (even if non-zero)
	sb := &Superblock{
		BlockCountLo:    32768,
		BlockCountHi:    9999, // garbage — should be ignored
		BlockPerGroup:   32768,
		FeatureIncompat: 0, // 32-bit mode
	}
	got := sb.GetGroupDescriptorTableCount()
	if got != 1 {
		t.Errorf("32-bit mode should ignore BlockCountHi, got %d, want 1", got)
	}
}

func TestGetGroupDescriptorCount(t *testing.T) {
	tests := []struct {
		name            string
		blockCountLo    uint32
		blockPerGroup   uint32
		logBlockSize    uint32
		featureIncompat uint32
		want            uint32
	}{
		{
			name:          "4KB blocks, 1 group: 1 block",
			blockCountLo:  32768,
			blockPerGroup: 32768,
			logBlockSize:  2, // 4096
			want:          1, // 1*32=32 bytes, ceil(32/4096)=1
		},
		{
			name:          "4KB blocks, 128 groups: fits in 1 block",
			blockCountLo:  128 * 32768,
			blockPerGroup: 32768,
			logBlockSize:  2,
			want:          1, // 128*32=4096 bytes, ceil(4096/4096)=1
		},
		{
			name:          "4KB blocks, 129 groups: needs 2 blocks",
			blockCountLo:  129 * 32768,
			blockPerGroup: 32768,
			logBlockSize:  2,
			want:          2, // 129*32=4128, ceil(4128/4096)=2
		},
		{
			name:          "1KB blocks, 32 groups: fits in 1 block",
			blockCountLo:  32 * 8192,
			blockPerGroup: 8192,
			logBlockSize:  0, // 1024
			want:          1, // 32*32=1024, ceil(1024/1024)=1
		},
		{
			name:          "1KB blocks, 33 groups: needs 2 blocks",
			blockCountLo:  33 * 8192,
			blockPerGroup: 8192,
			logBlockSize:  0,
			want:          2, // 33*32=1056, ceil(1056/1024)=2
		},
		{
			name:            "64bit mode: 64-byte descriptors",
			blockCountLo:    64 * 32768,
			blockPerGroup:   32768,
			logBlockSize:    2,
			featureIncompat: FEATURE_INCOMPAT_64BIT,
			want:            1, // 64*64=4096, ceil(4096/4096)=1
		},
		{
			name:            "64bit mode: 65 groups needs 2 blocks",
			blockCountLo:    65 * 32768,
			blockPerGroup:   32768,
			logBlockSize:    2,
			featureIncompat: FEATURE_INCOMPAT_64BIT,
			want:            2, // 65*64=4160, ceil(4160/4096)=2
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sb := &Superblock{
				BlockCountLo:    tt.blockCountLo,
				BlockPerGroup:   tt.blockPerGroup,
				LogBlockSize:    tt.logBlockSize,
				FeatureIncompat: tt.featureIncompat,
			}
			got := sb.GetGroupDescriptorCount()
			if got != tt.want {
				t.Errorf("GetGroupDescriptorCount() = %d, want %d (ngroups=%d)",
					got, tt.want, sb.GetGroupDescriptorTableCount())
			}
		})
	}
}

func TestGetGroupDescriptor_SeekOffset(t *testing.T) {
	// buildImage places a single 32-byte GD at the given byte offset.
	// The GD has InodeTableLo = marker so we can verify the correct offset
	// was read.
	buildImage := func(t *testing.T, gdOffset int, marker uint32) *io.SectionReader {
		t.Helper()
		imageSize := gdOffset + SectorSize // enough room for one sector read
		image := make([]byte, imageSize)

		gd := GroupDescriptor32{InodeTableLo: marker}
		buf := &bytes.Buffer{}
		if err := binary.Write(buf, binary.LittleEndian, &gd); err != nil {
			t.Fatal(err)
		}
		copy(image[gdOffset:], buf.Bytes())

		return io.NewSectionReader(bytes.NewReader(image), 0, int64(imageSize))
	}

	tests := []struct {
		name           string
		logBlockSize   uint32 // 0=1KB, 2=4KB
		firstDataBlock uint32
		blockCountLo   uint32
		blockPerGroup  uint32
		expectedOffset int // byte offset where GDT should be read
		marker         uint32
	}{
		{
			name:           "4KB blocks: GDT at block 1 (byte 4096)",
			logBlockSize:   2, // 1024<<2 = 4096
			firstDataBlock: 0,
			blockCountLo:   32768,
			blockPerGroup:  32768,
			expectedOffset: 4096,
			marker:         0xAAAA,
		},
		{
			name:           "1KB blocks: GDT at block 2 (byte 2048)",
			logBlockSize:   0, // 1024<<0 = 1024
			firstDataBlock: 1,
			blockCountLo:   8193,
			blockPerGroup:  8192,
			expectedOffset: 2048,
			marker:         0xBBBB,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := buildImage(t, tt.expectedOffset, tt.marker)

			sb := Superblock{
				LogBlockSize:   tt.logBlockSize,
				FirstDataBlock: tt.firstDataBlock,
				BlockCountLo:   tt.blockCountLo,
				BlockPerGroup:  tt.blockPerGroup,
			}

			gds, err := sb.getGroupDescriptor(*r)
			if err != nil {
				t.Fatalf("getGroupDescriptor() error: %v", err)
			}
			if len(gds) != 1 {
				t.Fatalf("expected 1 GD, got %d", len(gds))
			}
			if gds[0].InodeTableLo != tt.marker {
				t.Errorf("InodeTableLo = %#x, want %#x (GD read from wrong offset?)",
					gds[0].InodeTableLo, tt.marker)
			}
		})
	}
}
