package ext4

import "testing"

func TestInodeFileType(t *testing.T) {
	tests := []struct {
		name            string
		mode            uint16
		wantDir         bool
		wantRegular     bool
		wantSymlink     bool
		wantSocket      bool
		wantFifo        bool
		wantCharDevice  bool
		wantBlockDevice bool
	}{
		{
			name:        "regular file (0100644)",
			mode:        0100644,
			wantRegular: true,
		},
		{
			name:    "directory (040755)",
			mode:    040755,
			wantDir: true,
		},
		{
			name:        "symlink (0120777)",
			mode:        0120777,
			wantSymlink: true,
		},
		{
			name:       "socket (0140755)",
			mode:       0140755,
			wantSocket: true,
		},
		{
			name:     "fifo (010644)",
			mode:     010644,
			wantFifo: true,
		},
		{
			name:           "char device (020666)",
			mode:           020666,
			wantCharDevice: true,
		},
		{
			name:            "block device (060660)",
			mode:            060660,
			wantBlockDevice: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inode := Inode{Mode: tt.mode}
			if got := inode.IsDir(); got != tt.wantDir {
				t.Errorf("IsDir() = %v, want %v (mode: %#o)", got, tt.wantDir, tt.mode)
			}
			if got := inode.IsRegular(); got != tt.wantRegular {
				t.Errorf("IsRegular() = %v, want %v (mode: %#o)", got, tt.wantRegular, tt.mode)
			}
			if got := inode.IsSymlink(); got != tt.wantSymlink {
				t.Errorf("IsSymlink() = %v, want %v (mode: %#o)", got, tt.wantSymlink, tt.mode)
			}
			if got := inode.IsSocket(); got != tt.wantSocket {
				t.Errorf("IsSocket() = %v, want %v (mode: %#o)", got, tt.wantSocket, tt.mode)
			}
			if got := inode.IsFifo(); got != tt.wantFifo {
				t.Errorf("IsFifo() = %v, want %v (mode: %#o)", got, tt.wantFifo, tt.mode)
			}
			if got := inode.IsCharDevice(); got != tt.wantCharDevice {
				t.Errorf("IsCharDevice() = %v, want %v (mode: %#o)", got, tt.wantCharDevice, tt.mode)
			}
			if got := inode.IsBlockDevice(); got != tt.wantBlockDevice {
				t.Errorf("IsBlockDevice() = %v, want %v (mode: %#o)", got, tt.wantBlockDevice, tt.mode)
			}
		})
	}
}

func TestExtent_IsUninitialized(t *testing.T) {
	tests := []struct {
		name   string
		len    uint16
		wantUn bool
		wantN  uint16
	}{
		{"initialized: 1 block", 1, false, 1},
		{"initialized: max (0x7FFF)", 0x7FFF, false, 0x7FFF},
		{"uninitialized: 1 block (0x8001)", 0x8001, true, 1},
		{"uninitialized: max (0xFFFF)", 0xFFFF, true, 0x7FFF},
		{"zero length", 0, false, 0},
		{"exactly 0x8000 (uninitialized, 0 blocks)", 0x8000, true, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := Extent{Len: tt.len}
			if got := e.IsUninitialized(); got != tt.wantUn {
				t.Errorf("IsUninitialized() = %v, want %v", got, tt.wantUn)
			}
			if got := e.GetLen(); got != tt.wantN {
				t.Errorf("GetLen() = %d, want %d", got, tt.wantN)
			}
		})
	}
}

func TestInodeFileTypeMutualExclusion(t *testing.T) {
	modes := []struct {
		name string
		mode uint16
	}{
		{"regular", 0100644},
		{"directory", 040755},
		{"symlink", 0120777},
		{"socket", 0140755},
		{"fifo", 010644},
		{"char device", 020666},
		{"block device", 060660},
	}

	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			inode := Inode{Mode: m.mode}
			count := 0
			if inode.IsDir() {
				count++
			}
			if inode.IsRegular() {
				count++
			}
			if inode.IsSymlink() {
				count++
			}
			if inode.IsSocket() {
				count++
			}
			if inode.IsFifo() {
				count++
			}
			if inode.IsCharDevice() {
				count++
			}
			if inode.IsBlockDevice() {
				count++
			}
			if count != 1 {
				t.Errorf("expected exactly 1 type match for mode %#o, got %d", m.mode, count)
			}
		})
	}
}
