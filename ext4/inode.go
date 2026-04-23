package ext4

import (
	"bytes"
	"encoding/binary"

	"golang.org/x/xerrors"
)

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

// IsUninitialized returns true if this extent is unwritten (allocated but
// not yet written). Reads from such extents should return zeros.
func (e *Extent) IsUninitialized() bool {
	return e.Len&0x8000 != 0
}

// GetLen returns the actual number of blocks, masking off the
// uninitialized flag in bit 15.
func (e *Extent) GetLen() uint16 {
	return e.Len & 0x7FFF
}

// DirectoryEntry2 is more or less a flat file that maps an arbitrary byte string
type DirectoryEntry2 struct {
	Inode   uint32 `struc:"uint32,little"`
	RecLen  uint16 `struc:"uint16,little"`
	NameLen uint8  `struc:"uint8,sizeof=Name"`
	Flags   uint8  `struc:"uint8"`
	Name    string `struc:"[]byte"`
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

type BlockAddressing struct {
	DirectBlock         [12]uint32 `struc:"[12]uint32,little"`
	SingleIndirectBlock uint32     `struc:"uint32,little"`
	DoubleIndirectBlock uint32     `struc:"uint32,little"`
	TripleIndirectBlock uint32     `struc:"uint32,little"`
}

func (i *Inode) IsDir() bool {
	return (i.Mode & FileTypeMask) == FileTypeDir
}

func (i *Inode) IsRegular() bool {
	return (i.Mode & FileTypeMask) == FileTypeRegular
}

func (i *Inode) IsSocket() bool {
	return (i.Mode & FileTypeMask) == FileTypeSocket
}

func (i *Inode) IsSymlink() bool {
	return (i.Mode & FileTypeMask) == FileTypeSymlink
}

func (i *Inode) IsFifo() bool {
	return (i.Mode & FileTypeMask) == FileTypeFifo
}

func (i *Inode) IsCharDevice() bool {
	return (i.Mode & FileTypeMask) == FileTypeCharDevice
}

func (i *Inode) IsBlockDevice() bool {
	return (i.Mode & FileTypeMask) == FileTypeBlockDevice
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

// readIndirectBlockPointers reads all block pointers from an indirect block
// using ReadAt. Returns up to entriesPerBlock (blockSize/4) entries including
// zeros (sparse holes).
func readIndirectBlockPointers(ext4 *FileSystem, blockAddr uint32, remaining int64) ([]uint32, error) {
	blockSize := ext4.sb.GetBlockSize()
	buf := make([]byte, blockSize)
	_, err := ext4.r.ReadAt(buf, int64(blockAddr)*blockSize)
	if err != nil {
		return nil, xerrors.Errorf("failed to read indirect block at %#x: %w", blockAddr, err)
	}

	entriesPerBlock := blockSize / 4
	count := remaining
	if count > entriesPerBlock {
		count = entriesPerBlock
	}

	addrs := make([]uint32, count)
	for j := int64(0); j < count; j++ {
		addrs[j] = binary.LittleEndian.Uint32(buf[j*4 : j*4+4])
	}
	return addrs, nil
}

func resolveSingleIndirectBlockAddress(ext4 *FileSystem, blockAddr uint32, remaining int64) ([]uint32, error) {
	return readIndirectBlockPointers(ext4, blockAddr, remaining)
}

func resolveDoubleIndirectBlockAddress(ext4 *FileSystem, blockAddr uint32, remaining int64) ([]uint32, error) {
	blockSize := ext4.sb.GetBlockSize()
	entriesPerBlock := blockSize / 4

	pointers, err := readIndirectBlockPointers(ext4, blockAddr, entriesPerBlock)
	if err != nil {
		return nil, xerrors.Errorf("failed to read double indirect block: %w", err)
	}

	var blockAddresses []uint32
	for _, singleAddr := range pointers {
		if remaining <= 0 {
			break
		}
		if singleAddr == 0 {
			// Null pointer: emit zeros for the entire single indirect range
			zeros := remaining
			if zeros > entriesPerBlock {
				zeros = entriesPerBlock
			}
			blockAddresses = append(blockAddresses, make([]uint32, zeros)...)
			remaining -= zeros
			continue
		}
		addrs, err := resolveSingleIndirectBlockAddress(ext4, singleAddr, remaining)
		if err != nil {
			return nil, xerrors.Errorf("failed to resolve single indirect block: %w", err)
		}
		blockAddresses = append(blockAddresses, addrs...)
		remaining -= int64(len(addrs))
	}
	return blockAddresses, nil
}

func resolveTripleIndirectBlockAddress(ext4 *FileSystem, blockAddr uint32, remaining int64) ([]uint32, error) {
	blockSize := ext4.sb.GetBlockSize()
	entriesPerBlock := blockSize / 4
	blocksPerDouble := entriesPerBlock * entriesPerBlock

	pointers, err := readIndirectBlockPointers(ext4, blockAddr, entriesPerBlock)
	if err != nil {
		return nil, xerrors.Errorf("failed to read triple indirect block: %w", err)
	}

	var blockAddresses []uint32
	for _, doubleAddr := range pointers {
		if remaining <= 0 {
			break
		}
		if doubleAddr == 0 {
			// Null pointer: emit zeros for the entire double indirect range
			zeros := remaining
			if zeros > blocksPerDouble {
				zeros = blocksPerDouble
			}
			blockAddresses = append(blockAddresses, make([]uint32, zeros)...)
			remaining -= zeros
			continue
		}
		addrs, err := resolveDoubleIndirectBlockAddress(ext4, doubleAddr, remaining)
		if err != nil {
			return nil, xerrors.Errorf("failed to resolve double indirect block: %w", err)
		}
		blockAddresses = append(blockAddresses, addrs...)
		remaining -= int64(len(addrs))
	}
	return blockAddresses, nil
}

func (i *Inode) GetBlockAddresses(ext4 *FileSystem) ([]uint32, error) {
	blockSize := ext4.sb.GetBlockSize()
	totalBlocks := (i.GetSize() + blockSize - 1) / blockSize
	if totalBlocks == 0 {
		return nil, nil
	}

	addresses := BlockAddressing{}
	err := binary.Read(bytes.NewReader(i.BlockOrExtents[:]), binary.LittleEndian, &addresses)
	if err != nil {
		return nil, xerrors.Errorf("failed to read block addressing: %w", err)
	}

	blockAddresses := make([]uint32, 0, totalBlocks)

	// Direct blocks (up to 12)
	directCount := totalBlocks
	if directCount > 12 {
		directCount = 12
	}
	for j := int64(0); j < directCount; j++ {
		blockAddresses = append(blockAddresses, addresses.DirectBlock[j])
	}

	remaining := totalBlocks - int64(len(blockAddresses))

	if remaining > 0 && addresses.SingleIndirectBlock != 0 {
		addrs, err := resolveSingleIndirectBlockAddress(ext4, addresses.SingleIndirectBlock, remaining)
		if err != nil {
			return nil, xerrors.Errorf("failed to resolve single indirect block: %w", err)
		}
		blockAddresses = append(blockAddresses, addrs...)
		remaining -= int64(len(addrs))
	} else if remaining > 0 {
		// Null single indirect pointer: emit zeros
		entriesPerBlock := blockSize / 4
		zeros := remaining
		if zeros > entriesPerBlock {
			zeros = entriesPerBlock
		}
		blockAddresses = append(blockAddresses, make([]uint32, zeros)...)
		remaining -= zeros
	}

	if remaining > 0 && addresses.DoubleIndirectBlock != 0 {
		addrs, err := resolveDoubleIndirectBlockAddress(ext4, addresses.DoubleIndirectBlock, remaining)
		if err != nil {
			return nil, xerrors.Errorf("failed to resolve double indirect block: %w", err)
		}
		blockAddresses = append(blockAddresses, addrs...)
		remaining -= int64(len(addrs))
	} else if remaining > 0 {
		entriesPerBlock := blockSize / 4
		blocksPerDouble := entriesPerBlock * entriesPerBlock
		zeros := remaining
		if zeros > blocksPerDouble {
			zeros = blocksPerDouble
		}
		blockAddresses = append(blockAddresses, make([]uint32, zeros)...)
		remaining -= zeros
	}

	if remaining > 0 && addresses.TripleIndirectBlock != 0 {
		addrs, err := resolveTripleIndirectBlockAddress(ext4, addresses.TripleIndirectBlock, remaining)
		if err != nil {
			return nil, xerrors.Errorf("failed to resolve triple indirect block: %w", err)
		}
		blockAddresses = append(blockAddresses, addrs...)
	} else if remaining > 0 {
		blockAddresses = append(blockAddresses, make([]uint32, remaining)...)
	}

	return blockAddresses, nil
}

// DxRootInfo holds the hash tree root metadata, located at offset 0x18 in the
// root directory block (immediately after the dot and dotdot fake dirents).
type DxRootInfo struct {
	ReservedZero   uint32 `struc:"uint32,little"`
	HashVersion    uint8  `struc:"uint8"`
	InfoLength     uint8  `struc:"uint8"`
	IndirectLevels uint8  `struc:"uint8"`
	UnusedFlags    uint8  `struc:"uint8"`
}

// DxCountLimit is the count/limit header at the start of a dx_entry array.
// It reinterprets the first dx_entry: the first 2 bytes are limit, the next 2
// are count, followed by the block number for the leftmost subtree.
type DxCountLimit struct {
	Limit uint16 `struc:"uint16,little"`
	Count uint16 `struc:"uint16,little"`
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
