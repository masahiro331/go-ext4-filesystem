package ext4

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/lunixbochs/struc"
	"github.com/pkg/errors"
	"golang.org/x/xerrors"
)

// Reader is filesystem reader interface
type Reader interface {
	io.ReadCloser
	Next() (string, error)
}

// DataType is binary type
type DataType uint

// BlockSize is filesystem block size
const (
	BlockSize        = 0x400
	GroupZeroPadding = 0x400

	DirectoryFlag = 0x4000
	FileFlag      = 0x8000
	InlineFlag    = 0x10000000

	BlockBitmapFlag DataType = iota
	InodeBitmapFlag
	InodeTableFlag
	DirEntryFlag
	FileEntryFlag
	DataFlag
	Unknown
)

var (
	extentMap    = map[int64]*Extent{}
	dataMap      = map[int64]DataType{}
	fileMap      = FileMap{}
	inodeFileMap = map[int64]uint64{}
	inodes       = []Inode{}
)

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
		return nil, xerrors.New("unsupported block")
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
		return nil, fmt.Errorf("Block/inode mismatch: %d %d %d", sb.GetBlockCount(), numBlockGroups, numBlockGroups2)
	}

	rawbuffer := bytes.NewBuffer([]byte{})
	buf := make([]byte, BlockSize)
	// buf := make([]byte, sb.GetBlockSize())
	for i := uint32(0); i < sb.GetGroupDescriptorCount(); i++ {
		_, err := r.Read(buf)
		if err != nil {
			return nil, err
		}
		rawbuffer.Write(buf)
	}

	// Group Descriptors
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

		// ext4 Reader buffer
		buffer: bytes.NewBuffer([]byte{}),
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

// ExtendReader is vmdk file reader
func (ext4 *Ext4Reader) ExtendReader(p []byte) (int, error) {
	buf := make([]byte, BlockSize)
	inputBuffer := bytes.NewBuffer([]byte{})

	magnification := len(p) / int(ext4.sb.GetBlockSize())
	for i := 0; i < magnification; i++ {
		_, err := ext4.r.Read(buf)
		if err != nil {
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
func (ext4 *Ext4Reader) Next() (string, error) {
	buf := make([]byte, ext4.sb.GetBlockSize())

	for {
		// debug
		t, ok := dataMap[int64(ext4.pos)]
		if !ok {
			t = Unknown
		}

		switch t {
		case BlockBitmapFlag:
			_, err := ext4.r.Read(buf)
			if err != nil {
				if err == io.EOF {
					goto BREAK
				}
				return "", err
			}
			ext4.pos++

		case InodeBitmapFlag:
			_, err := ext4.r.Read(buf)
			if err != nil {
				if err == io.EOF {
					goto BREAK
				}
				return "", err
			}
			ext4.pos++

		case InodeTableFlag:
			inodeTableBlockCount := ext4.sb.InodePerGroup * uint32(ext4.sb.InodeSize) / uint32(ext4.sb.GetBlockSize())
			for i := uint32(0); i < inodeTableBlockCount; i++ {
				_, err := ext4.r.Read(buf)
				if err != nil {
					if err == io.EOF {
						goto BREAK
					}
					return "", err
				}
				blockReader := bytes.NewReader(buf)
				ext4.pos++

				for j := 0; j < len(buf)/int(ext4.sb.InodeSize); j++ {
					var inode Inode

					err := binary.Read(blockReader, binary.LittleEndian, &inode)
					if err != nil {
						return "", errors.Errorf("failed to read inode: %+v", err)
					}

					if inode.Mode != 0 {
						if inode.UsesExtents() {
							//log.Println("Finding", num)
							r := io.Reader(bytes.NewReader(inode.BlockOrExtents[:]))

							extentHeader := &ExtentHeader{}
							err := binary.Read(r, binary.LittleEndian, extentHeader)
							if err != nil {
								return "", errors.Errorf("failed to read inode block: %+v", err)
							}

							// if depth == 0, this node is Leaf
							if extentHeader.Depth == 0 {
								for entry := uint16(0); entry < extentHeader.Entries; entry++ {
									extent := &Extent{}
									err := binary.Read(r, binary.LittleEndian, extent)
									if err != nil {
										return "", errors.Errorf("failed to read leaf node extent: %+v", err)
									}

									if inode.Mode&DirectoryFlag != 0 {
										dataMap[int64(extent.StartHi<<32)+int64(extent.StartLo)] = DirEntryFlag
									} else if inode.Mode&FileFlag != 0 {
										dataMap[int64(extent.StartHi<<32)+int64(extent.StartLo)] = FileEntryFlag
										inodeFileMap[int64(extent.StartHi<<32)+int64(extent.StartLo)] = uint64(ext4.pos-1)*uint64(ext4.sb.GetBlockSize()) + uint64(j*int(ext4.sb.InodeSize))
									} else {
										dataMap[int64(extent.StartHi<<32)+int64(extent.StartLo)] = DataFlag
									}

									extentMap[int64(extent.StartHi<<32)+int64(extent.StartLo)] = extent
								}
							}
							// else {
							// 	// TODO: not support
							// }
						} else {
						}
					}
					inodes = append(inodes, inode)
				}
			}

		case DataFlag:
			_, err := ext4.r.Read(buf)
			if err != nil {
				if err == io.EOF {
					goto BREAK
				}
				return "", err
			}
			ext4.pos++
		case FileEntryFlag:
			extent, ok := extentMap[int64(ext4.pos)]
			if !ok {
				return "", errors.New("extent not found")
			}
			offset, ok := inodeFileMap[int64(ext4.pos)]
			if !ok {
				return "", errors.New("inode not found")
			}
			buf := make([]byte, int(ext4.sb.GetBlockSize())*int(extent.Len))
			_, err := ext4.r.Read(buf)
			if err != nil {
				if err == io.EOF {
					goto BREAK
				}
				return "", err
			}
			ext4.pos += uint32(extent.Len)

			//
			ext4.buffer = bytes.NewBuffer(buf)

			file, ok := fileMap[offset]
			if !ok {
				// TODO: why not found inode files...
				break
			}
			return file.Name, nil

		case DirEntryFlag:
			extent, ok := extentMap[int64(ext4.pos)]
			if !ok {
				return "", errors.New("extent not found")
			}

			buf := make([]byte, int(ext4.sb.GetBlockSize())*int(extent.Len))

			_, err := ext4.r.Read(buf)
			if err != nil {
				if err == io.EOF {
					goto BREAK
				}
				return "", err
			}

			directoryReader := bytes.NewReader(buf)
			dirEntry := DirectoryEntry2{}
			for {
				err = struc.Unpack(directoryReader, &dirEntry)
				if err != nil {
					if err == io.EOF {
						break
					}
					return "", errors.Errorf("failed to parse directory entry: %+v", err)
				}
				size := dirEntry.NameLen + 8
				padding := dirEntry.RecLen - uint16(size)

				//  det_reserved_ft
				if dirEntry.Flags == 0xDE {
					break
				}

				if (dirEntry.Inode-1)/ext4.sb.InodePerGroup > uint32(len(ext4.gds)) {
					return "", errors.New("inode address greater than gds length")
				}

				gd := ext4.gds[(dirEntry.Inode-1)/ext4.sb.InodePerGroup]
				index := int64((dirEntry.Inode - 1) % ext4.sb.InodePerGroup)
				pos := gd.GetInodeTableLoc(ext4.sb.FeatureInCompat64bit())*ext4.sb.GetBlockSize() + index*int64(ext4.sb.InodeSize)

				fileMap[uint64(pos)] = dirEntry

				// read padding
				directoryReader.Read(make([]byte, padding))
			}
			ext4.pos += uint32(extent.Len)
		case Unknown: // default
			_, err := ext4.r.Read(buf)
			if err != nil {
				if err == io.EOF {
					goto BREAK
				}
				return "", err
			}
			ext4.pos++
		}
	}
BREAK:

	return "", io.EOF
}

// Close is close filesystem reader
func (ext4 *Ext4Reader) Close() error {
	return nil
}
