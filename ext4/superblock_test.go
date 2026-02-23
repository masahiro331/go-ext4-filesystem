package ext4

import "testing"

func TestGetGroupDescriptorTableCount(t *testing.T) {
	tests := []struct {
		name           string
		blockCountLo   uint32
		blockCountHi   uint32
		blockPerGroup  uint32
		firstDataBlock uint32
		featureIncompat uint32 // set FEATURE_INCOMPAT_64BIT if needed
		want           uint32
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
			blockCountHi:    1,        // total = 1<<32 = 4294967296 blocks
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
