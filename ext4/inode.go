package ext4

// ExtentHeader is ...
type ExtentHeader struct {
	Magic      uint16 `struc:"uint16,little"`
	Entries    uint16 `struc:"uint16,little"`
	Max        uint16 `struc:"uint16,little"`
	Depth      uint16 `struc:"uint16,little"`
	Generation uint32 `struc:"uint32,little"`
}

// Extent is extent tree leaf nodes
type Extent struct {
	Block   uint32 `struc:"uint32,little"`
	Len     uint16 `struc:"uint16,little"`
	StartHi uint16 `struc:"uint16,little"`
	StartLo uint32 `struc:"uint32,little"`
}

// DirectoryEntry2 is more or less a flat file that maps an arbitrary byte string
type DirectoryEntry2 struct {
	Inode   uint32 `struc:"uint32,little"`
	RecLen  uint16 `struc:"uint16,little"`
	NameLen uint8  `struc:"uint8,sizeof=Name"`
	Flags   uint8  `struc:"uint8"`
	Name    string `struc:"[]byte"`
}

func (d *DirectoryEntry2) isFile() bool {
	return (d.Flags & DIR_ENTRY_FILE_TYPE_REGULAR_FILE) != 0
}

func (d *DirectoryEntry2) isDir() bool {
	return (d.Flags & DIR_ENTRY_FILE_TYPE_DIRECTORY) != 0
}

// Inode is index-node
type Inode struct {
	Mode           uint16   `struc:"uint16,little"`
	UID            uint16   `struc:"uint16,little"`
	SizeLo         uint32   `struc:"uint32,little"`
	Atime          uint32   `struc:"uint32,little"`
	Ctime          uint32   `struc:"uint32,little"`
	Mtime          uint32   `struc:"uint32,little"`
	Dtime          uint32   `struc:"uint32,little"`
	GID            uint16   `struc:"uint16,little"`
	LinksCount     uint16   `struc:"uint16,little"`
	BlocksLo       uint32   `struc:"uint32,little"`
	Flags          uint32   `struc:"uint32,little"`
	Osd1           uint32   `struc:"uint32,little"`
	BlockOrExtents [60]byte `struc:"[60]byte,little"`
	Generation     uint32   `struc:"uint32,little"`
	FileACLLo      uint32   `struc:"uint32,little"`
	SizeHigh       uint32   `struc:"uint32,little"`
	ObsoFaddr      uint32   `struc:"uint32,little"`
	// OSD2 - linux only starts
	BlocksHigh  uint16 `struc:"uint16,little"`
	FileACLHigh uint16 `struc:"uint16,little"`
	UIDHigh     uint16 `struc:"uint16,little"`
	GIDHigh     uint16 `struc:"uint16,little"`
	ChecksumLow uint16 `struc:"uint16,little"`
	Unused      uint16 `struc:"uint16,little"`
	// OSD2 - linux only ends
	ExtraIsize  uint16 `struc:"uint16,little"`
	ChecksumHi  uint16 `struc:"uint16,little"`
	CtimeExtra  uint32 `struc:"uint32,little"`
	MtimeExtra  uint32 `struc:"uint32,little"`
	AtimeExtra  uint32 `struc:"uint32,little"`
	Crtime      uint32 `struc:"uint32,little"`
	CrtimeExtra uint32 `struc:"uint32,little"`
	VersionHi   uint32 `struc:"uint32,little"`
	Projid      uint32 `struc:"uint32,little"`
	// padding
	Reserved [96]uint8 `struc:"[96]uint32,little"`
}

func (i Inode) IsDir() bool {
	return i.Mode&0x4000 != 0 && i.Mode&0x8000 == 0
}

func (i Inode) IsRegular() bool {
	return i.Mode&0x8000 != 0 && i.Mode&0x4000 == 0
}

func (i Inode) IsSocket() bool {
	return i.Mode&0xC000 != 0
}

func (i Inode) IsSymlink() bool {
	return i.Mode&0xA000 != 0
}

// UsesExtents
func (i *Inode) UsesExtents() bool {
	return (i.Flags & EXTENTS_FL) != 0
}

// UsesDirectoryHashTree
func (i *Inode) UsesDirectoryHashTree() bool {
	return (i.Flags & INDEX_FL) != 0
}

// GetSize is get inode file size
func (i *Inode) GetSize() int64 {
	return (int64(i.SizeHigh) << 32) | int64(i.SizeLo)
}

// ExtentInternal
type ExtentInternal struct {
	Block    uint32 `struc:"uint32,little"`
	LeafLow  uint32 `struc:"uint32,little"`
	LeafHigh uint16 `struc:"uint16,little"`
	Unused   uint16 `struc:"uint16,little"`
}

// DirectoryEntryCsum is not use
type DirectoryEntryCsum struct {
	FakeInodeZero uint32 `struc:"uint32,little"`
	RecLen        uint16 `struc:"uint16,little"`
	FakeNameLen   uint8  `struc:"uint8"`
	FakeFileType  uint8  `struc:"uint8"`
	Checksum      uint32 `struc:"uint32,little"`
}

// MoveExtent is not use
type MoveExtent struct {
	Reserved   uint32 `struc:"uint32,little"`
	DonorFd    uint32 `struc:"uint32,little"`
	OrigStart  uint64 `struc:"uint64,little"`
	DonorStart uint64 `struc:"uint64,little"`
	Len        uint64 `struc:"uint64,little"`
	MovedLen   uint64 `struc:"uint64,little"`
}
