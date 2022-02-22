package ext4

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"golang.org/x/xerrors"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"

	"github.com/lunixbochs/struc"
	"github.com/pkg/errors"
)

// Reader is filesystem reader interface
type Reader interface {
	io.ReadCloser
	Next() (*File, error)
}

// DataType is binary type
type DataType uint

// BlockSize is filesystem block size
const (
	SectorSize       = 0x200
	BlockSize        = 0x400
	SuperBlockSize   = 0x400
	GroupZeroPadding = 0x400

	DirectoryFlag = 0x4000
	FileFlag      = 0x8000
	InlineFlag    = 0x10000000
)

const (
	rootInodeNumber = 2

	BlockBitmapFlag DataType = iota
	InodeBitmapFlag
	InodeTableFlag
	DirEntryFlag
	FileEntryFlag
	DataFlag
	Unknown
)

func (sb Superblock) getGroupDescriptor(r io.SectionReader) ([]GroupDescriptor, error) {
	_, err := r.Seek(sb.GetBlockSize(), 0)
	if err != nil {
		return nil, xerrors.Errorf("failed to seek Group Descriptor offset: %w", err)
	}

	GroupDescriptorSize := int(sb.GetGroupDescriptorTableCount()) * 32
	if sb.FeatureInCompat64bit() {
		GroupDescriptorSize = int(sb.GetGroupDescriptorTableCount()) * 64
	}

	count := divWithRoundUp(GroupDescriptorSize, SectorSize)
	if err != nil {
		return nil, xerrors.Errorf("failed to div: %w", err)
	}
	buf, err := readBlock(&r, int64(count)*SectorSize)
	if err != nil {
		return nil, xerrors.Errorf("failed to read group descriptor: %w", err)
	}
	var gds []GroupDescriptor
	for i := uint32(0); i < sb.GetGroupDescriptorTableCount(); i++ {
		var gd GroupDescriptor
		err = binary.Read(buf, binary.LittleEndian, &gd)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse group descriptor: %w", err)
		}
		gds = append(gds, gd)
	}

	return gds, nil
}

func (ext4 *FileSystem) getInode(inodeAddress int64) (*Inode, error) {
	bgd := ext4.gds[(inodeAddress-1)/int64(ext4.sb.InodePerGroup)]
	index := (inodeAddress - 1) % int64(ext4.sb.InodePerGroup)
	physicalOffset := bgd.GetInodeTableLoc(ext4.sb.FeatureInCompat64bit())*ext4.sb.GetBlockSize() + index*int64(ext4.sb.InodeSize)

	// offset need to 512*N offset
	inodeOffset := physicalOffset % SectorSize
	seekOffset := physicalOffset - (physicalOffset % SectorSize)
	buf := make([]byte, SectorSize)
	_, err := ext4.r.ReadAt(buf, seekOffset)
	if err != nil {
		return nil, xerrors.Errorf("failed to read inode: %w", err)
	}
	if inodeOffset != 0 && int64(len(buf)) > inodeOffset {
		buf = buf[inodeOffset:]
	}

	inode := &Inode{}
	if err := binary.Read(bytes.NewReader(buf), binary.LittleEndian, inode); err != nil {
		return nil, xerrors.Errorf("failed to read binary: %w", err)
	}
	return inode, nil
}

func (ext4 *FileSystem) extents(b []byte, extents []Extent) ([]Extent, error) {
	extentReader := bytes.NewReader(b)
	extentHeader := &ExtentHeader{}
	err := binary.Read(extentReader, binary.LittleEndian, extentHeader)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse extent header: %w", err)
	}

	if extentHeader.Depth == 0 {
		for entry := uint16(0); entry < extentHeader.Entries; entry++ {
			var extent Extent
			err := binary.Read(extentReader, binary.LittleEndian, &extent)
			if err != nil {
				return nil, errors.Errorf("failed to read leaf node extent: %+v", err)
			}
			extents = append(extents, extent)
		}
	} else {
		for i := uint16(0); i < extentHeader.Entries; i++ {
			var extent ExtentInternal
			err := binary.Read(extentReader, binary.LittleEndian, &extent)
			if err != nil {
				return nil, errors.Errorf("failed to read internal extent: %+v", err)
			}
			b := make([]byte, SectorSize)
			_, err = ext4.r.ReadAt(b, int64(extent.LeafHigh)<<32+int64(extent.LeafLow)*ext4.sb.GetBlockSize())
			if err != nil {
				return nil, errors.Errorf("failed to read leaf node extent: %+v", err)
			}

			extents, err = ext4.extents(b, extents)
			if err != nil {
				return nil, errors.Errorf("failed to get extents: %+v", err)
			}
		}
	}
	return extents, nil
}

func (ext4 *FileSystem) Extents(inode *Inode) ([]Extent, error) {
	extents, err := ext4.extents(inode.BlockOrExtents[:], nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to get extents: %w", err)
	}

	return extents, nil
}

func (ext4 *FileSystem) dump() {
	b := make([]byte, SectorSize)
	_, err := ext4.r.Read(b)
	if err != nil {
		log.Fatal(err)
	}
	hex.Dumper(os.Stdout).Write(b)
}

func (e *Extent) offset() int64 {
	return int64(e.StartHi)<<32 + int64(e.StartLo)
}

func divWithRoundUp(a int, b int) int {
	n := a / b
	if a%b != 0 {
		return n + 1
	}
	return n
}

func readBlock(r io.Reader, size int64) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	for i := int64(0); i < size/SectorSize; i++ {
		n, err := io.CopyN(buf, r, SectorSize)
		if err != nil {
			return nil, xerrors.Errorf("failed to read block: %w", err)
		}
		if n != SectorSize {
			return nil, xerrors.New("failed to read sector")
		}
	}
	return buf, nil
}

// ===========================================================-
// FileMap is not block Offset address key is file offset
type FileMap map[uint64]DirectoryEntry2

// NewReader is create filesystem reader
func NewReader(r io.Reader) (Reader, error) {
	block := make([]byte, GroupZeroPadding)

	// first block is boot sector
	_, err := r.Read(block)
	if err != nil {
		return nil, err
	}

	// only ext4 support
	return NewExt4Reader(r)
}

// Ext4Reader is ext4 filesystem reader
type Ext4Reader struct {
	r io.Reader

	extentMap        map[int64]*Extent
	dataMap          map[int64]DataType
	fileMap          FileMap
	inodeFileMap     map[int64]uint64
	inodeMap         map[int64]Inode
	inodeFileNameMap map[uint32]string

	buffer *bytes.Buffer
	sb     Superblock
	gds    []GroupDescriptor
	pos    uint32
}

/*
Ext4 Block Layout
+-----------------+------------------+-------------------+---------------------+-------------------+--------------+-------------+------------------+
| Group 0 Padding | ext4 Super Block | Group Descriptors | Reserved GDT Blocks | Data Block Bitmap | inode Bitmap | inode Table	| Data Blocks      |
+-----------------+------------------+-------------------+---------------------+-------------------+--------------+-------------+------------------+
| 1024 bytes      | 1 block	         | many blocks       | many blocks         | 1 block           | 1 block      | many blocks | many more blocks |
+-----------------+------------------+-------------------+---------------------+-------------------+--------------+-------------+------------------+
*/

// NewExt4Reader is create Ext4Reader
func NewExt4Reader(r io.Reader) (Reader, error) {
	// ext4 Super Block
	var sb Superblock
	if err := binary.Read(r, binary.LittleEndian, &sb); err != nil {
		return nil, err
	}
	if sb.Magic != 0xEF53 {
		return nil, errors.New("unsupported block")
	}

	// Read padding block
	// SuperBlock size is filesystem block size
	// 1 block = LogBlockSize * 1024
	// padding zize = block - 1024(suplerblock) - padding(1024)
	if sb.GetBlockSize() != BlockSize {
		_, err := r.Read(make([]byte, sb.GetBlockSize()-BlockSize*2))
		if err != nil {
			return nil, err
		}
	}

	numBlockGroups := (sb.GetBlockCount() + int64(sb.BlockPerGroup) - 1) / int64(sb.BlockPerGroup)
	numBlockGroups2 := (sb.InodeCount + sb.InodePerGroup - 1) / sb.InodePerGroup
	if numBlockGroups != int64(numBlockGroups2) {
		return nil, errors.Errorf("Block/inode mismatch: %d %d %d", sb.GetBlockCount(), numBlockGroups, numBlockGroups2)
	}

	rawbuffer := bytes.Buffer{}
	buf := make([]byte, BlockSize)
	for i := uint32(0); i < sb.GetGroupDescriptorCount(); i++ {
		_, err := r.Read(buf)
		if err != nil {
			return nil, err
		}
		rawbuffer.Write(buf)
	}

	// Group Descriptors
	dataMap := map[int64]DataType{}
	var gds []GroupDescriptor
	for i := uint32(0); i < sb.GetGroupDescriptorTableCount(); i++ {
		var size uint32
		if sb.FeatureInCompat64bit() {
			size = 64
		} else {
			size = 32
		}
		tmpbuf := make([]byte, size)
		_, err := rawbuffer.Read(tmpbuf)
		if err != nil {
			return nil, errors.Errorf("raw buffer error %+v", err)
		}
		if len(tmpbuf) == 32 {
			tmpbuf = append(tmpbuf, make([]byte, 32)...)
		}

		var gd GroupDescriptor
		err = binary.Read(bytes.NewReader(tmpbuf), binary.LittleEndian, &gd)
		if err != nil {
			return nil, errors.Errorf("failed to parse group descriptor: %+v", err)
		}
		dataMap[gd.GetBlockBitmapLoc(sb.FeatureInCompat64bit())] = BlockBitmapFlag
		dataMap[gd.GetInodeBitmapLoc(sb.FeatureInCompat64bit())] = InodeBitmapFlag
		dataMap[gd.GetInodeTableLoc(sb.FeatureInCompat64bit())] = InodeTableFlag
		gds = append(gds, gd)
	}

	buf = make([]byte, sb.GetBlockSize())
	for i := uint16(0); i < sb.ReservedGdtBlocks; i++ {
		_, err := r.Read(buf)
		if err != nil {
			return nil, err
		}
	}

	pos := 1 + uint32(sb.ReservedGdtBlocks) + (sb.GetGroupDescriptorCount() / (uint32(sb.GetBlockSize()) / BlockSize))
	ext4Reader := &Ext4Reader{
		// input reader
		r: r,

		dataMap:          dataMap,
		extentMap:        map[int64]*Extent{},
		fileMap:          FileMap{},
		inodeFileMap:     map[int64]uint64{},
		inodeMap:         map[int64]Inode{},
		inodeFileNameMap: map[uint32]string{rootInodeNumber: ""},

		// ext4 Reader buffer
		buffer: &bytes.Buffer{},
		sb:     sb,
		gds:    gds,
		pos:    pos,
	}

	return ext4Reader, nil
}

// Read is read file bytes
func (ext4 *Ext4Reader) Read(p []byte) (int, error) {
	return ext4.buffer.Read(p)
}

// ExtendRead is vmdk file reader
func (ext4 *Ext4Reader) ExtendRead(p []byte) (int, error) {

	buf := make([]byte, BlockSize)
	inputBuffer := bytes.Buffer{}

	magnification := len(p) / BlockSize
	for i := 0; i < magnification; i++ {
		_, err := ext4.r.Read(buf)
		if err != nil {
			if err == io.EOF {
				return 0, io.EOF
			}
			return 0, errors.Errorf("failed to extend read: %+v", err)
		}
		_, err = inputBuffer.Write(buf)
		if err != nil {
			return 0, errors.Errorf("failed to write buffer: %+v", err)
		}
	}
	return inputBuffer.Read(p)
}

// Next is return next read filename
func (ext4 *Ext4Reader) Next() (*File, error) {
	buf := make([]byte, ext4.sb.GetBlockSize())

	for {
		// debug
		t, ok := ext4.dataMap[int64(ext4.pos)]
		if !ok {
			t = Unknown
		}

		switch t {
		case BlockBitmapFlag:
			_, err := ext4.ExtendRead(buf)
			if err != nil {
				if err == io.EOF {
					goto BREAK
				}
				return nil, err
			}
			ext4.pos++

		case InodeBitmapFlag:
			_, err := ext4.ExtendRead(buf)
			if err != nil {
				if err == io.EOF {
					goto BREAK
				}
				return nil, err
			}
			ext4.pos++

		case InodeTableFlag:
			inodeTableBlockCount := ext4.sb.InodePerGroup * uint32(ext4.sb.InodeSize) / uint32(ext4.sb.GetBlockSize())
			for i := uint32(0); i < inodeTableBlockCount; i++ {
				_, err := ext4.ExtendRead(buf)
				if err != nil {
					if err == io.EOF {
						goto BREAK
					}
					return nil, err
				}
				blockReader := bytes.NewReader(buf)
				ext4.pos++

				for j := 0; j < len(buf)/int(ext4.sb.InodeSize); j++ {
					var inode Inode

					err := binary.Read(blockReader, binary.LittleEndian, &inode)
					if err != nil {
						return nil, errors.Errorf("failed to read inode: %+v", err)
					}

					if inode.Mode == 0 {
						continue
					}
					if !inode.UsesExtents() {
						continue
					}

					r := io.Reader(bytes.NewReader(inode.BlockOrExtents[:]))
					extentHeader := &ExtentHeader{}
					err = binary.Read(r, binary.LittleEndian, extentHeader)
					if err != nil {
						return nil, errors.Errorf("failed to read inode block: %+v", err)
					}

					if extentHeader.Depth != 0 {
						// if depth == 0 is Leaf Node
						// if depth > 0 is internal extent
						// internal extent is not support
						continue
					}
					for entry := uint16(0); entry < extentHeader.Entries; entry++ {
						extent := &Extent{}
						err := binary.Read(r, binary.LittleEndian, extent)
						if err != nil {
							return nil, errors.Errorf("failed to read leaf node extent: %+v", err)
						}

						if inode.Mode&DirectoryFlag != 0 {
							ext4.dataMap[int64(extent.StartHi<<32)+int64(extent.StartLo)] = DirEntryFlag
						} else if inode.Mode&FileFlag != 0 {
							ext4.dataMap[int64(extent.StartHi<<32)+int64(extent.StartLo)] = FileEntryFlag
							ext4.inodeFileMap[int64(extent.StartHi<<32)+int64(extent.StartLo)] = uint64(ext4.pos-1)*uint64(ext4.sb.GetBlockSize()) + uint64(j*int(ext4.sb.InodeSize))
							ext4.inodeMap[int64(extent.StartHi<<32)+int64(extent.StartLo)] = inode
						} else {
							ext4.dataMap[int64(extent.StartHi<<32)+int64(extent.StartLo)] = DataFlag
						}
						ext4.extentMap[int64(extent.StartHi<<32)+int64(extent.StartLo)] = extent
					}
				}
			}

		case DataFlag:
			_, err := ext4.ExtendRead(buf)
			if err != nil {
				if err == io.EOF {
					goto BREAK
				}
				return nil, err
			}
			ext4.pos++
		case FileEntryFlag:
			offset, ok := ext4.inodeFileMap[int64(ext4.pos)]
			if !ok {
				return nil, errors.New("inode offset not found")
			}
			inode, ok := ext4.inodeMap[int64(ext4.pos)]
			if !ok {
				return nil, errors.New("inode not found")
			}

			blockCount := int64(math.Ceil(float64(inode.GetSize()) / float64(ext4.sb.GetBlockSize())))

			// ReDefinition buf
			buf := make([]byte, int(ext4.sb.GetBlockSize()*blockCount))
			_, err := ext4.ExtendRead(buf)
			if err != nil {
				if err == io.EOF {
					goto BREAK
				}
				return nil, err
			}
			ext4.pos += uint32(blockCount)

			ext4.buffer = bytes.NewBuffer(buf[:inode.GetSize()])

			file, ok := ext4.fileMap[offset]
			if !ok {
				// TODO: why not found inode files...
				break
			}
			return &File{
				filePath: file.Name,
				mTime:    inode.Mtime,
				mode:     inode.Mode,
				size:     inode.GetSize(),
				isDir:    false,
			}, nil

		case DirEntryFlag:
			extent, ok := ext4.extentMap[int64(ext4.pos)]
			if !ok {
				return nil, errors.New("extent not found")
			}

			buf := make([]byte, int(ext4.sb.GetBlockSize())*int(extent.Len))

			_, err := ext4.ExtendRead(buf)
			if err != nil {
				if err == io.EOF {
					goto BREAK
				}
				return nil, err
			}

			directoryReader := bytes.NewReader(buf)
			dirEntry := DirectoryEntry2{}
			var currentInode uint32
			for {
				err = struc.Unpack(directoryReader, &dirEntry)
				if err != nil {
					if err == io.EOF {
						break
					}
					return nil, errors.Errorf("failed to parse directory entry: %+v", err)
				}
				size := dirEntry.NameLen + 8
				padding := dirEntry.RecLen - uint16(size)

				//  det_reserved_ft
				if dirEntry.Flags == 0xDE {
					break
				}

				if (dirEntry.Inode-1)/ext4.sb.InodePerGroup > uint32(len(ext4.gds)) {
					return nil, errors.New("inode address greater than gds length")
				}

				gd := ext4.gds[(dirEntry.Inode-1)/ext4.sb.InodePerGroup]
				index := int64((dirEntry.Inode - 1) % ext4.sb.InodePerGroup)
				pos := gd.GetInodeTableLoc(ext4.sb.FeatureInCompat64bit())*ext4.sb.GetBlockSize() + index*int64(ext4.sb.InodeSize)

				if dirEntry.Name == "." {
					currentInode = dirEntry.Inode
				} else if dirEntry.Name == ".." {
					// parentInode = dirEntry.Inode
				} else {
					absPath := filepath.Join(ext4.inodeFileNameMap[currentInode], dirEntry.Name)
					ext4.inodeFileNameMap[dirEntry.Inode] = absPath
					dirEntry.Name = absPath
				}

				// TODO: ???
				// // File
				// if 0x1&dirEntry.Flags != 0 {
				// }
				// // Directory
				// if 0x2&dirEntry.Flags != 0 {
				// }

				ext4.fileMap[uint64(pos)] = dirEntry

				// read padding
				directoryReader.Read(make([]byte, padding))
			}
			ext4.pos += uint32(extent.Len)
		case Unknown: // default
			_, err := ext4.ExtendRead(buf)
			if err != nil {
				if err == io.EOF {
					goto BREAK
				}
				return nil, err
			}
			ext4.pos++
		}
	}
BREAK:

	return nil, io.EOF
}

// Close is close filesystem reader
func (ext4 *Ext4Reader) Close() error {
	return nil
}
