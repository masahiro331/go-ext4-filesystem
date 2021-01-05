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
	Next() error
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

// FileMap is not block Offset address key is file offset
type FileMap map[uint32]DirectoryEntry2

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
		// fmt.Println("---------------------------------------------")
		// fmt.Println(gd.GetBlockBitmapLoc(sb.FeatureInCompat64bit()))
		// fmt.Println(gd.GetInodeBitmapLoc(sb.FeatureInCompat64bit()))
		// fmt.Println(gd.GetInodeTableLoc(sb.FeatureInCompat64bit()))
		// fmt.Println(gd)
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
		r:      r,
		buffer: bytes.NewBuffer([]byte{}),
		sb:     sb,
		gds:    gds,
		pos:    pos,
	}

	extentMap := map[int64]*Extent{}
	dataMap := map[int64]DataType{}

	// 1831619 alpine-release i-node number

	// 7340076 alpine-relase inode pos (gd)
	// 7372940 alpine-relase position

	// bg := (inodeAddress-1) / sb.InodePerGroup
	// index := (inodeAddress - 1) % int64(fs.sb.InodePer_group)
	// pos := bgd.GetInodeTableLoc()*fs.sb.GetBlockSize() + index*int64(fs.sb.Inode_size)
	// block address := pos / 4096  もしoffsetが0より大きければ (+1)
	// block offset := pos % 4096

	for _, gd := range gds {
		dataMap[gd.GetBlockBitmapLoc(sb.FeatureInCompat64bit())] = BlockBitmapFlag
		dataMap[gd.GetInodeBitmapLoc(sb.FeatureInCompat64bit())] = InodeBitmapFlag
		dataMap[gd.GetInodeTableLoc(sb.FeatureInCompat64bit())] = InodeTableFlag
	}

	fileMap := FileMap{}
	inodeFileMap := map[int64]uint32{}
	inodes := []Inode{}
	for {
		// debug
		t, ok := dataMap[int64(pos)]
		if !ok {
			t = Unknown
		}

		switch t {
		case BlockBitmapFlag:
			_, err := r.Read(buf)
			if err != nil {
				if err == io.EOF {
					goto BREAK
				}
				return nil, err
			}
			pos++

		case InodeBitmapFlag:
			_, err := r.Read(buf)
			if err != nil {
				if err == io.EOF {
					goto BREAK
				}
				return nil, err
			}
			pos++

		case InodeTableFlag:
			inodeTableBlockCount := sb.InodePerGroup * uint32(sb.InodeSize) / uint32(sb.GetBlockSize())
			for i := uint32(0); i < inodeTableBlockCount; i++ {
				_, err := r.Read(buf)
				if err != nil {
					if err == io.EOF {
						goto BREAK
					}
					return nil, err
				}
				blockReader := bytes.NewReader(buf)
				pos++

				for j := 0; j < len(buf)/int(sb.InodeSize); j++ {
					var inode Inode

					err := binary.Read(blockReader, binary.LittleEndian, &inode)
					if err != nil {
						return nil, errors.Errorf("failed to read inode: %+v", err)
					}

					if inode.Mode != 0 {
						if inode.UsesExtents() {
							//log.Println("Finding", num)
							r := io.Reader(bytes.NewReader(inode.BlockOrExtents[:]))

							extentHeader := &ExtentHeader{}
							err := binary.Read(r, binary.LittleEndian, extentHeader)
							if err != nil {
								return nil, errors.Errorf("failed to read inode block: %+v", err)
							}

							// if depth == 0, this node is Leaf
							if extentHeader.Depth == 0 {
								for entry := uint16(0); entry < extentHeader.Entries; entry++ {
									extent := &Extent{}
									err := binary.Read(r, binary.LittleEndian, extent)
									if err != nil {
										return nil, errors.Errorf("failed to read leaf node extent: %+v", err)
									}

									if inode.Mode&DirectoryFlag != 0 {
										dataMap[int64(extent.StartHi<<32)+int64(extent.StartLo)] = DirEntryFlag
									} else if inode.Mode&FileFlag != 0 {
										dataMap[int64(extent.StartHi<<32)+int64(extent.StartLo)] = FileEntryFlag
										inodeFileMap[int64(extent.StartHi<<32)+int64(extent.StartLo)] = pos*uint32(sb.GetBlockSize()) + uint32(j*int(sb.InodeSize))
									} else {
										dataMap[int64(extent.StartHi<<32)+int64(extent.StartLo)] = DataFlag
									}
									extentMap[int64(extent.StartHi<<32)+int64(extent.StartLo)] = extent
								}
							}
							// else {
							// 	// TODO: not support
							// 	for i := uint16(0); i < extentHeader.Entries; i++ {
							// 		extentInternal := &ExtentInternal{}
							// 		err := binary.Read(r, binary.LittleEndian, extentInternal)
							// 		if err != nil {
							// 			return nil, errors.Errorf("failed to read internal extent: %+v", err)
							// 		}
							// 		fmt.Println("extent internal:", extentInternal)
							// 	}

							// }
						} else {
							// dirEntry := &DirectoryEntry2{}
							// r := io.Reader(bytes.NewReader(inode.BlockOrExtents[:]))
							// struc.Unpack(r, dirEntry)
							// fmt.Println("entry:", dirEntry)
						}
					}
					inodes = append(inodes, inode)
				}
			}

		case DataFlag:
			_, err := r.Read(buf)
			if err != nil {
				if err == io.EOF {
					goto BREAK
				}
				return nil, err
			}
			pos++
		case FileEntryFlag:
			offset, ok := inodeFileMap[int64(pos)]
			if !ok {
				return nil, errors.New("inode not found")
			}
			file, ok := fileMap[offset]
			if !ok {
				return nil, errors.New("file not found")
			}
			fmt.Println("===== file ====", file)

			extent, ok := extentMap[int64(pos)]
			if !ok {
				return nil, errors.New("extent not found")
			}

			buf := make([]byte, int(sb.GetBlockSize())*int(extent.Len))
			_, err := r.Read(buf)
			if err != nil {
				if err == io.EOF {
					goto BREAK
				}
				return nil, err
			}
			pos += uint32(extent.Len)
		case DirEntryFlag:
			extent, ok := extentMap[int64(pos)]
			if !ok {
				return nil, errors.New("extent not found")
			}

			buf := make([]byte, int(sb.GetBlockSize())*int(extent.Len))

			_, err := r.Read(buf)
			if err != nil {
				if err == io.EOF {
					goto BREAK
				}
				return nil, err
			}

			directoryReader := bytes.NewReader(buf)
			dirEntry := DirectoryEntry2{}
			for {
				err = struc.Unpack(directoryReader, &dirEntry)
				if err != nil {
					return nil, errors.Errorf("failed to parse directory entry: %+v", err)
				}
				size := dirEntry.NameLen + 8
				padding := dirEntry.RecLen - uint16(size)

				//  det_reserved_ft
				if dirEntry.Flags == 0xDE {
					break
				}

				if (dirEntry.Inode-1)/sb.InodePerGroup > uint32(len(gds)) {
					panic("inode address greater than gds length")
				}

				gd := gds[(dirEntry.Inode-1)/sb.InodePerGroup]
				index := int64((dirEntry.Inode - 1) % sb.InodePerGroup)
				pos := gd.GetInodeTableLoc(sb.FeatureInCompat64bit())*sb.GetBlockSize() + index*int64(sb.InodeSize)

				fileMap[uint32(pos)] = dirEntry

				// bg := (inodeAddress-1) / sb.InodePerGroup
				// index := (inodeAddress - 1) % int64(fs.sb.InodePer_group)
				// pos := bgd.GetInodeTableLoc()*fs.sb.GetBlockSize() + index*int64(fs.sb.Inode_size)
				// block address := pos / 4096  もしoffsetが0より大きければ (+1)
				// block offset := pos % 4096

				// read padding
				directoryReader.Read(make([]byte, padding))
			}
			pos += uint32(extent.Len)
		case Unknown: // default
			_, err := r.Read(buf)
			if err != nil {
				if err == io.EOF {
					goto BREAK
				}
				return nil, err
			}
			pos++
		}
	}
BREAK:

	// for _, inode := range inodes {
	// }

	//log.Printf("extent header: %+v", extentHeader)
	// if extentHeader.Depth == 0 { // Leaf
	// 		if int64(extent.Block) <= num && int64(extent.Block)+int64(extent.Len) > num {
	// 			//log.Println("Found")
	// 			return int64(extent.Start_hi<<32) + int64(extent.Start_lo) + num - int64(extent.Block), int64(extent.Block) + int64(extent.Len) - num, true
	// 		}
	// } else {
	// 	found := false
	// 	for i := uint16(0); i < extentHeader.Entries; i++ {
	// 		extent := &ExtentInternal{}
	// 		struc.Unpack(r, &extent)
	// 		//log.Printf("extent internal: %+v", extent)
	// 		if int64(extent.Block) <= num {
	// 			newBlock := int64(extent.Leaf_high<<32) + int64(extent.Leaf_low)
	// 			inode.fs.dev.Seek(newBlock * inode.fs.sb.GetBlockSize(), 0)
	// 			r = inode.fs.dev
	// 			found = true
	// 			break
	// 		}
	// 	}
	// 	if !found {
	// 		return 0,0, false
	// 	}
	// }

	// if num < 12 {
	// 	return int64(binary.LittleEndian.Uint32(inode.BlockOrExtents[4*num:])), 1, true
	// }

	// num -= 12

	// indirectsPerBlock := inode.fs.sb.GetBlockSize() / 4
	// if num < indirectsPerBlock {
	// 	ptr := int64(binary.LittleEndian.Uint32(inode.BlockOrExtents[4*12:]))
	// 	return inode.getIndirectBlockPtr(ptr, num),1, true
	// }
	// num -= indirectsPerBlock

	// if num < indirectsPerBlock * indirectsPerBlock {
	// 	ptr := int64(binary.LittleEndian.Uint32(inode.BlockOrExtents[4*13:]))
	// 	l1 := inode.getIndirectBlockPtr(ptr, num / indirectsPerBlock)
	// 	return inode.getIndirectBlockPtr(l1, num % indirectsPerBlock),1, true
	// }

	// num -= indirectsPerBlock * indirectsPerBlock

	// if num < indirectsPerBlock * indirectsPerBlock * indirectsPerBlock {
	// 	log.Println("Triple indirection")

	// 	ptr := int64(binary.LittleEndian.Uint32(inode.BlockOrExtents[4*14:]))
	// 	l1 := inode.getIndirectBlockPtr(ptr, num / (indirectsPerBlock * indirectsPerBlock))
	// 	l2 := inode.getIndirectBlockPtr(l1, (num / indirectsPerBlock) % indirectsPerBlock)
	// 	return inode.getIndirectBlockPtr(l2, num % (indirectsPerBlock * indirectsPerBlock)),1, true
	// }

	// log.Fatalf("Exceeded maximum possible block count")
	// return 0,0,false

	// 	}
	// }

	return ext4Reader, nil
	// s_reserved_gdt_blocks
}

// func getInode(sb Superblock, inodeAddress int64) int64 {
// 	bgd := getBlockGroupDescriptor(sb, (inodeAddress-1)/int64(sb.InodePerGroup))
// 	index := (inodeAddress - 1) % int64(sb.InodePerGroup)
// 	pos := bgd.GetInodeTableLoc(sb.FeatureInCompat64bit())*sb.GetBlockSize() + index*int64(sb.InodeSize)
// 	//log.Printf("%d %d %d %d", bgd.GetInodeTableLoc(), fs.sb.GetBlockSize(), index, fs.sb.Inode_size)
// 	return pos
// }
//
// func getBlockGroupDescriptor(sb Superblock, blockGroupNum int64) *GroupDescriptor {
// 	blockSize := sb.GetBlockSize()
// 	bgdtLocation := 1024/blockSize + 1
//
// 	size := int64(32)
// 	if sb.FeatureInCompat64bit() {
// 		size = int64(64)
// 	}
// 	addr := bgdtLocation*blockSize + size*blockGroupNum
// 	bgd := &GroupDescriptor{
// 		fs:      fs,
// 		address: addr,
// 		num:     blockGroupNum,
// 	}
// 	fs.dev.Seek(addr, 0)
// 	struc.Unpack(io.LimitReader(fs.dev, size), &bgd)
// 	//log.Printf("Read block group %d, contents:\n%+v\n", blockGroupNum, bgd)
// 	return bgd
// }

// DOC Group Descriptors
/*
オフセット	サイズ	名前			説明
0x0		__le32	bg_block_bitmap_lo	ブロックビットマップの位置の下位32ビット。
0x4		__le32	bg_inode_bitmap_lo	iノードビットマップの位置の下位32ビット。
0x8		__le32	bg_inode_table_lo	iノードテーブルの位置の下位32ビット。
0xC		__le16	bg_free_blocks_count_lo	下位16ビットのフリーブロック数。
0xE		__le16	bg_free_inodes_count_lo	空きiノード数の下位16ビット。
0x10		__le16	bg_used_dirs_count_lo	ディレクトリカウントの下位16ビット。
0x12		__le16	bg_flags		ブロックグループフラグ。のいずれか：
						0x1	iノードテーブルとビットマップは初期化されていません（EXT4_BG_INODE_UNINIT）。
						0x2	ブロックビットマップは初期化されていません（EXT4_BG_BLOCK_UNINIT）。
						0x4	inodeテーブルはゼロ化されます（EXT4_BG_INODE_ZEROED）。
0x14		__le32	bg_exclude_bitmap_lo	スナップショット除外ビットマップの位置の下位32ビット。
0x18		__le16	bg_block_bitmap_csum_lo	ブロックビットマップチェックサムの下位16ビット。
0x1A		__le16	bg_inode_bitmap_csum_lo	iノードビットマップチェックサムの下位16ビット。
0x1C		__le16	bg_itable_unused_lo	未使用のiノード数の下位16ビット。設定されている場合(sb.s_inodes_per_group - gdt.bg_itable_unused)、このグループのiノードテーブルのth番目のエントリをスキャンする必要はありません。
0x1E		__le16	bg_checksum		グループ記述子のチェックサム。RO_COMPAT_GDT_CSUM機能が設定されている場合はcrc16（sb_uuid + group + desc）、RO_COMPAT_METADATA_CSUM機能が設定されている場合はcrc32c（sb_uuid + group_desc）＆0xFFFF。
これらの	フィールドは、64ビット機能が有効でs_desc_size> 32の場合にのみ存在します。
0x20		__le32	bg_block_bitmap_hi	ブロックビットマップの位置の上位32ビット。
0x24		__le32	bg_inode_bitmap_hi	iノードのビットマップの位置の上位32ビット。
0x28		__le32	bg_inode_table_hi	iノードテーブルの位置の上位32ビット。
0x2C		__le16	bg_free_blocks_count_hi	空きブロックカウントの上位16ビット。
0x2E		__le16	bg_free_inodes_count_hi	空きiノード数の上位16ビット。
0x30		__le16	bg_used_dirs_count_hi	ディレクトリカウントの上位16ビット。
0x32		__le16	bg_itable_unused_hi	未使用のiノード数の上位16ビット。
0x34		__le32	bg_exclude_bitmap_hi	スナップショット除外ビットマップの場所の上位32ビット。
0x38		__le16	bg_block_bitmap_csum_hi	ブロックビットマップチェックサムの上位16ビット。
0x3A		__le16	bg_inode_bitmap_csum_hi	iノードビットマップチェックサムの上位16ビット。
0x3C		__u32	bg_reserved		64バイトにパディングします。
*/

// sample

/*
{
0x0  259    ブロックビットマップの位置の下位32ビット。
0x4  272    iノードビットマップの位置の下位32ビット。
0x8  285    iノードテーブルの位置の下位32ビット。
0xC  4683   下位16ビットのフリーブロック数。
0xE  1952   空きiノード数の下位16ビット。
0x10 2      ディレクトリカウントの下位16ビット。
0x12 4      ブロックグループフラグ。のいずれか：
            0x1	iノードテーブルとビットマップは初期化されていません（EXT4_BG_INODE_UNINIT）。
            0x2	ブロックビットマップは初期化されていません（EXT4_BG_BLOCK_UNINIT）。
            0x4	inodeテーブルはゼロ化されます（EXT4_BG_INODE_ZEROED）。
0x14 0      スナップショット除外ビットマップの位置の下位32ビット。
0x18 64232  ブロックビットマップチェックサムの下位16ビット。
0x1A 20514  iノードビットマップチェックサムの下位16ビット。
0x1C 1951   未使用のiノード数の下位16ビット。設定されている場合(sb.s_inodes_per_group - gdt.bg_itable_unused)、このグループのiノードテーブルのth番目のエントリをスキャンする必要はありません。
0x1E 61735  グループ記述子のチェックサム。RO_COMPAT_GDT_CSUM機能が設定されている場合はcrc16（sb_uuid + group + desc）、RO_COMPAT_METADATA_CSUM機能が設定されている場合はcrc32c（sb_uuid + group_desc）＆0xFFFF。
0x20 0
0x24 0
0x28 0
0x2C 0
0x2E 0
0x30 0
0x32 0
0x34 0
0x38 0
0x3A 0
0x3C 0
}
{260 273 532 3521 1976 0 5 0 59990 0 1976 4786 0 0 0 0 0 0 0 0 0 0 0}
{261 274 779 1435 1976 0 5 0 16114 0 1976 49814 0 0 0 0 0 0 0 0 0 0 0}
{262 275 1026 7751 1976 0 5 0 59623 0 1976 53664 0 0 0 0 0 0 0 0 0 0 0}
{263 276 1273 8192 1976 0 5 0 23897 0 1976 32384 0 0 0 0 0 0 0 0 0 0 0}
{264 277 1520 7934 1976 0 5 0 38899 0 1976 48803 0 0 0 0 0 0 0 0 0 0 0}
{265 278 1767 4096 1976 0 5 0 12694 0 1976 33546 0 0 0 0 0 0 0 0 0 0 0}
{266 279 2014 7934 1976 0 5 0 38899 0 1976 15313 0 0 0 0 0 0 0 0 0 0 0}
{267 280 2261 8192 1976 0 5 0 23897 0 1976 12569 0 0 0 0 0 0 0 0 0 0 0}
{268 281 2508 7934 1976 0 5 0 38899 0 1976 1512 0 0 0 0 0 0 0 0 0 0 0}
{269 282 2755 8192 1976 0 5 0 23897 0 1976 13438 0 0 0 0 0 0 0 0 0 0 0}
{270 283 3002 8192 1976 0 5 0 23897 0 1976 30762 0 0 0 0 0 0 0 0 0 0 0}
{271 284 3249 4095 1976 0 5 0 58098 0 1976 33883 0 0 0 0 0 0 0 0 0 0 0}
*/

// Read is read filesystem
func (ext4 *Ext4Reader) Read(p []byte) (int, error) {
	return 0, nil
}

// Next is return next file
func (ext4 *Ext4Reader) Next() error {

	return nil
}

// Close is close filesystem reader
func (ext4 *Ext4Reader) Close() error {
	return nil
}
