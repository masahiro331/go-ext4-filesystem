package ext4

type MoveExtent struct {
	Reserved    uint32 `struc:"uint32,little"`
	Donor_fd    uint32 `struc:"uint32,little"`
	Orig_start  uint64 `struc:"uint64,little"`
	Donor_start uint64 `struc:"uint64,little"`
	Len         uint64 `struc:"uint64,little"`
	Moved_len   uint64 `struc:"uint64,little"`
}

type ExtentHeader struct {
	Magic      uint16 `struc:"uint16,little"`
	Entries    uint16 `struc:"uint16,little"`
	Max        uint16 `struc:"uint16,little"`
	Depth      uint16 `struc:"uint16,little"`
	Generation uint32 `struc:"uint32,little"`
}

type ExtentInternal struct {
	Block     uint32 `struc:"uint32,little"`
	Leaf_low  uint32 `struc:"uint32,little"`
	Leaf_high uint16 `struc:"uint16,little"`
	Unused    uint16 `struc:"uint16,little"`
}

type Extent struct {
	Block   uint32 `struc:"uint32,little"`
	Len     uint16 `struc:"uint16,little"`
	StartHi uint16 `struc:"uint16,little"`
	StartLo uint32 `struc:"uint32,little"`
}

type DirectoryEntry2 struct {
	Inode   uint32 `struc:"uint32,little"`
	RecLen  uint16 `struc:"uint16,little"`
	NameLen uint8  `struc:"uint8,sizeof=Name"`
	Flags   uint8  `struc:"uint8"`
	Name    string `struc:"[]byte"`
}

type DirectoryEntryCsum struct {
	FakeInodeZero uint32 `struc:"uint32,little"`
	Rec_len       uint16 `struc:"uint16,little"`
	FakeName_len  uint8  `struc:"uint8"`
	FakeFileType  uint8  `struc:"uint8"`
	Checksum      uint32 `struc:"uint32,little"`
}

type Inode struct {
	Mode           uint16   `struc:"uint16,little"`
	Uid            uint16   `struc:"uint16,little"`
	Size_lo        uint32   `struc:"uint32,little"`
	Atime          uint32   `struc:"uint32,little"`
	Ctime          uint32   `struc:"uint32,little"`
	Mtime          uint32   `struc:"uint32,little"`
	Dtime          uint32   `struc:"uint32,little"`
	Gid            uint16   `struc:"uint16,little"`
	Links_count    uint16   `struc:"uint16,little"`
	Blocks_lo      uint32   `struc:"uint32,little"`
	Flags          uint32   `struc:"uint32,little"`
	Osd1           uint32   `struc:"uint32,little"`
	BlockOrExtents [60]byte `struc:"[60]byte,little"`
	Generation     uint32   `struc:"uint32,little"`
	File_acl_lo    uint32   `struc:"uint32,little"`
	Size_high      uint32   `struc:"uint32,little"`
	Obso_faddr     uint32   `struc:"uint32,little"`
	// OSD2 - linux only starts
	Blocks_high   uint16 `struc:"uint16,little"`
	File_acl_high uint16 `struc:"uint16,little"`
	Uid_high      uint16 `struc:"uint16,little"`
	Gid_high      uint16 `struc:"uint16,little"`
	Checksum_low  uint16 `struc:"uint16,little"`
	Unused        uint16 `struc:"uint16,little"`
	// OSD2 - linux only ends
	Extra_isize  uint16 `struc:"uint16,little"`
	Checksum_hi  uint16 `struc:"uint16,little"`
	Ctime_extra  uint32 `struc:"uint32,little"`
	Mtime_extra  uint32 `struc:"uint32,little"`
	Atime_extra  uint32 `struc:"uint32,little"`
	Crtime       uint32 `struc:"uint32,little"`
	Crtime_extra uint32 `struc:"uint32,little"`
	Version_hi   uint32 `struc:"uint32,little"`
	Projid       uint32 `struc:"uint32,little"`
	// padding
	Reserved [96]uint8 `struc:"[96]uint32,little"`
}

func (inode *Inode) UsesExtents() bool {
	return (inode.Flags & EXTENTS_FL) != 0
}

func (inode *Inode) UsesDirectoryHashTree() bool {
	return (inode.Flags & INDEX_FL) != 0
}
