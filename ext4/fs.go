package ext4

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/fs"
	"path"
	"path/filepath"
	"strings"

	"github.com/lunixbochs/struc"
	"golang.org/x/xerrors"
)

var (
	_ fs.FS        = &FileSystem{}
	_ fs.ReadDirFS = &FileSystem{}
	_ fs.StatFS    = &FileSystem{}
)

// FileSystem is implemented io/fs interface
type FileSystem struct {
	r *io.SectionReader

	sb  Superblock
	gds []GroupDescriptor

	cache Cache[string, any]
}

func readPadding(r io.Reader) error {
	_, err := readBlock(r, GroupZeroPadding)
	if err != nil {
		return err
	}
	return nil
}

func parseSuperBlock(r io.Reader) (Superblock, error) {
	err := readPadding(r)
	if err != nil {
		return Superblock{}, xerrors.Errorf("failed to seek padding: %w", err)
	}
	b, err := readBlock(r, 1024)
	if err != nil {
		return Superblock{}, xerrors.Errorf("failed to read super block: %w", err)
	}
	var sb Superblock
	if err := binary.Read(b, binary.LittleEndian, &sb); err != nil {
		return Superblock{}, xerrors.Errorf("failed to binary read super block: %w", err)
	}
	if sb.Magic != 0xEF53 {
		return Superblock{}, xerrors.New("unsupported block")
	}
	return sb, nil
}

// NewFS is created io/fs.FS for ext4 filesystem
func NewFS(r io.SectionReader, cache Cache[string, any]) (*FileSystem, error) {
	sb, err := parseSuperBlock(&r)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse super block: %w", err)
	}

	numBlockGroups := int64(sb.GetGroupDescriptorTableCount())
	numBlockGroups2 := (sb.InodeCount + sb.InodePerGroup - 1) / sb.InodePerGroup
	if numBlockGroups != int64(numBlockGroups2) {
		return nil, xerrors.Errorf("Block/inode mismatch: %d %d %d", sb.GetBlockCount(), numBlockGroups, numBlockGroups2)
	}

	gds, err := sb.getGroupDescriptor(r)
	if err != nil {
		return nil, xerrors.Errorf("failed to get group Descriptor: %w", err)
	}

	if cache == nil {
		cache = &mockCache[string, any]{}
	}
	fs := &FileSystem{
		r:     &r,
		sb:    sb,
		gds:   gds,
		cache: cache,
	}
	return fs, nil
}

func (ext4 *FileSystem) ReadDir(path string) ([]fs.DirEntry, error) {
	const op = "read directory"

	dirEntries, err := ext4.readDirEntry(path)
	if err != nil {
		return nil, ext4.wrapError(op, path, err)
	}
	return dirEntries, nil
}

func (ext4 *FileSystem) readDirEntry(name string) ([]fs.DirEntry, error) {
	fileInfos, err := ext4.listFileInfo(rootInodeNumber)
	if err != nil {
		return nil, xerrors.Errorf("failed to list file infos: %w", err)
	}

	var currentIno int64
	cleanedPath := filepath.ToSlash(filepath.Clean(name))
	dirs := strings.Split(strings.Trim(cleanedPath, "/"), "/")
	if len(dirs) == 1 && (dirs[0] == "." || dirs[0] == "") {
		var dirEntries []fs.DirEntry
		for _, fileInfo := range fileInfos {
			if fileInfo.Name() == "." || fileInfo.Name() == ".." {
				continue
			}
			dirEntries = append(dirEntries, dirEntry{fileInfo})
		}
		return dirEntries, nil
	}

	for i, dir := range dirs {
		found := false
		for _, fileInfo := range fileInfos {
			if fileInfo.Name() != dir {
				continue
			}
			if !fileInfo.IsDir() {
				return nil, xerrors.Errorf("%s is file, directory: %w", fileInfo.Name(), fs.ErrNotExist)
			}
			found = true
			currentIno = fileInfo.ino
		}

		if !found {
			return nil, fs.ErrNotExist
		}

		fileInfos, err = ext4.listFileInfo(currentIno)
		if err != nil {
			return nil, xerrors.Errorf("failed to list directory entries inode(%d): %w", currentIno, err)
		}
		if i != len(dirs)-1 {
			continue
		}

		var dirEntries []fs.DirEntry
		for _, fileInfo := range fileInfos {
			// Skip current directory and parent directory
			// infinit loop in walkDir
			if fileInfo.Name() == "." || fileInfo.Name() == ".." {
				continue
			}

			dirEntries = append(dirEntries, dirEntry{fileInfo})
		}
		return dirEntries, nil
	}
	return nil, fs.ErrNotExist
}

func (ext4 *FileSystem) listFileInfo(ino int64) ([]FileInfo, error) {
	entries, err := ext4.listEntries(ino)
	if err != nil {
		return nil, xerrors.Errorf("failed to get directory entries: %w", err)
	}

	var fileInfos []FileInfo
	for _, entry := range entries {
		inode, err := ext4.getInode(int64(entry.Inode))
		if err != nil {
			return nil, xerrors.Errorf("failed to get inode(%d): %w", entry.Inode, err)
		}
		fileInfos = append(fileInfos,
			FileInfo{
				name:  entry.Name,
				ino:   int64(entry.Inode),
				inode: inode,
			},
		)
	}
	return fileInfos, nil
}

func extractDirectoryEntries(directoryReader *bytes.Buffer) ([]DirectoryEntry2, error) {
	var dirEntries []DirectoryEntry2

	for {
		dirEntry := DirectoryEntry2{}

		err := struc.Unpack(directoryReader, &dirEntry)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, xerrors.Errorf("failed to parse directory entry: %w", err)
		}

		if dirEntry.RecLen == 0 {
			break
		}

		nameAndHeader := uint16(dirEntry.NameLen) + 8
		if dirEntry.RecLen < nameAndHeader {
			break
		}
		align := dirEntry.RecLen - nameAndHeader
		if int(align) > directoryReader.Len() {
			break
		}
		_, err = directoryReader.Read(make([]byte, align))
		if err != nil {
			return nil, xerrors.Errorf("failed to read align: %w", err)
		}

		// inode == 0 means the entry is unused (deleted or padding such as checksum tail).
		if dirEntry.Inode == 0 {
			continue
		}
		if dirEntry.Name == "." || dirEntry.Name == ".." {
			continue
		}

		dirEntries = append(dirEntries, dirEntry)
	}

	return dirEntries, nil
}

// buildDirectoryBlockMap builds a mapping from logical directory block numbers
// to physical byte offsets for the given inode.
func (ext4 *FileSystem) buildDirectoryBlockMap(inode *Inode) (map[uint32]int64, error) {
	blockSize := ext4.sb.GetBlockSize()
	m := make(map[uint32]int64)

	if inode.UsesExtents() {
		extents, err := ext4.Extents(inode)
		if err != nil {
			return nil, xerrors.Errorf("failed to get extents: %w", err)
		}
		for _, e := range extents {
			if e.IsUninitialized() {
				return nil, xerrors.Errorf("failed to build directory block map: uninitialized extent at logical block %d", e.Block)
			}
			for i := uint32(0); i < uint32(e.GetLen()); i++ {
				m[e.Block+i] = (e.offset() + int64(i)) * blockSize
			}
		}
	} else {
		blockAddresses, err := inode.GetBlockAddresses(ext4)
		if err != nil {
			return nil, xerrors.Errorf("failed to get block addresses: %w", err)
		}
		for i, addr := range blockAddresses {
			if addr == 0 {
				continue
			}
			m[uint32(i)] = int64(addr) * blockSize
		}
	}

	return m, nil
}

// readLogicalBlock reads a single logical block using a pre-built block map.
func (ext4 *FileSystem) readLogicalBlock(blockMap map[uint32]int64, logicalBlock uint32) ([]byte, error) {
	offset, ok := blockMap[logicalBlock]
	if !ok {
		return nil, xerrors.Errorf("logical block %d not found in block map", logicalBlock)
	}

	buf := make([]byte, ext4.sb.GetBlockSize())
	_, err := ext4.r.ReadAt(buf, offset)
	if err != nil {
		return nil, xerrors.Errorf("failed to read block at offset %#x: %w", offset, err)
	}
	return buf, nil
}

// parseDxBlockNumbers extracts logical block numbers from dx_entry data.
// data starts right after the DxCountLimit (at the block field of the header entry).
func parseDxBlockNumbers(data []byte, count uint16) []uint32 {
	blocks := make([]uint32, 0, count)

	if count == 0 {
		return blocks
	}

	// Header entry: only the block field (4 bytes, no hash)
	if len(data) < 4 {
		return blocks
	}
	blocks = append(blocks, binary.LittleEndian.Uint32(data[:4]))

	// Remaining entries: each 8 bytes (hash:4 + block:4)
	for i := uint16(1); i < count; i++ {
		off := int(4 + (i-1)*8)
		if off+8 > len(data) {
			break
		}
		blocks = append(blocks, binary.LittleEndian.Uint32(data[off+4:off+8]))
	}

	return blocks
}

// collectLeafBlocks recursively traverses HTree internal nodes to collect
// all leaf block numbers.
func (ext4 *FileSystem) collectLeafBlocks(blockMap map[uint32]int64, nodeBlocks []uint32, remainingDepth uint8) ([]uint32, error) {
	if remainingDepth == 0 {
		return nodeBlocks, nil
	}

	var leafBlocks []uint32
	for _, nodeBlock := range nodeBlocks {
		data, err := ext4.readLogicalBlock(blockMap, nodeBlock)
		if err != nil {
			return nil, xerrors.Errorf("failed to read internal node block %d: %w", nodeBlock, err)
		}

		// Internal node layout:
		// 0x00-0x07: fake dirent (8 bytes)
		// 0x08-0x0B: DxCountLimit (4 bytes)
		// 0x0C+: dx_entry data (block0 + remaining entries)
		if len(data) < 0x0C {
			return nil, xerrors.New("htree internal node block too small")
		}

		var cl DxCountLimit
		if err := binary.Read(bytes.NewReader(data[0x08:0x0C]), binary.LittleEndian, &cl); err != nil {
			return nil, xerrors.Errorf("failed to parse dx_countlimit in internal node: %w", err)
		}
		if cl.Count > cl.Limit {
			return nil, xerrors.Errorf("htree internal node: count (%d) exceeds limit (%d)", cl.Count, cl.Limit)
		}

		childBlocks := parseDxBlockNumbers(data[0x0C:], cl.Count)

		leaves, err := ext4.collectLeafBlocks(blockMap, childBlocks, remainingDepth-1)
		if err != nil {
			return nil, err
		}
		leafBlocks = append(leafBlocks, leaves...)
	}

	return leafBlocks, nil
}

// listEntriesHTree reads all directory entries from an HTree-indexed directory
// by traversing the hash tree structure and reading only leaf blocks.
func (ext4 *FileSystem) listEntriesHTree(inode *Inode) ([]DirectoryEntry2, error) {
	blockMap, err := ext4.buildDirectoryBlockMap(inode)
	if err != nil {
		return nil, xerrors.Errorf("failed to build block map: %w", err)
	}

	// Read root block (logical block 0)
	rootData, err := ext4.readLogicalBlock(blockMap, 0)
	if err != nil {
		return nil, xerrors.Errorf("failed to read htree root block: %w", err)
	}

	// Root block layout:
	// 0x00-0x0B: dot entry (12 bytes)
	// 0x0C-0x17: dotdot entry (12 bytes)
	// 0x18-0x1F: DxRootInfo (8 bytes)
	// 0x20-0x23: DxCountLimit (4 bytes)
	// 0x24+: dx_entry data (block0 + remaining entries)
	if len(rootData) < 0x24 {
		return nil, xerrors.New("htree root block too small")
	}

	var rootInfo DxRootInfo
	if err := binary.Read(bytes.NewReader(rootData[0x18:0x20]), binary.LittleEndian, &rootInfo); err != nil {
		return nil, xerrors.Errorf("failed to parse dx_root_info: %w", err)
	}
	maxLevels := uint8(2)
	if ext4.sb.FeatureIncompatLargedir() {
		maxLevels = 3
	}
	if rootInfo.IndirectLevels > maxLevels {
		return nil, xerrors.Errorf("htree indirect_levels (%d) exceeds maximum (%d)", rootInfo.IndirectLevels, maxLevels)
	}

	var cl DxCountLimit
	if err := binary.Read(bytes.NewReader(rootData[0x20:0x24]), binary.LittleEndian, &cl); err != nil {
		return nil, xerrors.Errorf("failed to parse dx_countlimit: %w", err)
	}
	if cl.Count > cl.Limit {
		return nil, xerrors.Errorf("htree root: count (%d) exceeds limit (%d)", cl.Count, cl.Limit)
	}

	// Collect block numbers from root dx_entries
	rootBlocks := parseDxBlockNumbers(rootData[0x24:], cl.Count)

	// Traverse tree to collect all leaf block numbers
	leafBlocks, err := ext4.collectLeafBlocks(blockMap, rootBlocks, rootInfo.IndirectLevels)
	if err != nil {
		return nil, xerrors.Errorf("failed to collect htree leaf blocks: %w", err)
	}

	// Read directory entries from each leaf block
	var entries []DirectoryEntry2
	for _, logBlock := range leafBlocks {
		data, err := ext4.readLogicalBlock(blockMap, logBlock)
		if err != nil {
			return nil, xerrors.Errorf("failed to read leaf block %d: %w", logBlock, err)
		}

		dirEntries, err := extractDirectoryEntries(bytes.NewBuffer(data))
		if err != nil {
			return nil, xerrors.Errorf("failed to extract directory entries from leaf block %d: %w", logBlock, err)
		}
		entries = append(entries, dirEntries...)
	}

	return entries, nil
}

func (ext4 *FileSystem) listEntries(ino int64) ([]DirectoryEntry2, error) {
	inode, err := ext4.getInode(ino)
	if err != nil {
		return nil, xerrors.Errorf("failed to get inode(%d): %w", ino, err)
	}

	if inode.UsesDirectoryHashTree() {
		return ext4.listEntriesHTree(inode)
	}

	if !inode.UsesExtents() {
		var dirEntries []DirectoryEntry2

		blockAddresses, err := inode.GetBlockAddresses(ext4)
		if err != nil {
			return nil, xerrors.Errorf("failed to get block address: %w", err)
		}

		blockSize := ext4.sb.GetBlockSize()
		for _, blockAddress := range blockAddresses {
			if blockAddress == 0 {
				continue
			}

			buf := make([]byte, blockSize)
			_, err = ext4.r.ReadAt(buf, int64(blockAddress)*blockSize)
			if err != nil {
				return nil, xerrors.Errorf("failed to read directory block at %#x: %w", blockAddress, err)
			}

			extracted, err := extractDirectoryEntries(bytes.NewBuffer(buf))
			if err != nil {
				return nil, xerrors.Errorf("failed to extract directory entries: %w", err)
			}
			dirEntries = append(dirEntries, extracted...)
		}
		return dirEntries, nil
	}

	extents, err := ext4.Extents(inode)
	if err != nil {
		return nil, xerrors.Errorf("failed to get extents: %w", err)
	}

	blockSize := ext4.sb.GetBlockSize()
	var entries []DirectoryEntry2
	for _, e := range extents {
		if e.IsUninitialized() {
			return nil, xerrors.Errorf("failed to list directory entries: uninitialized extent at logical block %d", e.Block)
		}
		size := blockSize * int64(e.GetLen())
		buf := make([]byte, size)
		_, err := ext4.r.ReadAt(buf, e.offset()*blockSize)
		if err != nil {
			return nil, xerrors.Errorf("failed to read directory blocks at offset %#x: %w", e.offset()*blockSize, err)
		}

		dirEntries, err := extractDirectoryEntries(bytes.NewBuffer(buf))
		if err != nil {
			return nil, xerrors.Errorf("failed to extract directory entries: %w", err)
		}
		entries = append(entries, dirEntries...)
	}
	return entries, nil
}

func (ext4 *FileSystem) Stat(name string) (fs.FileInfo, error) {
	const op = "stat"

	f, err := ext4.Open(name)
	if err != nil {
		info, err := ext4.ReadDirInfo(name)
		if err != nil {
			return nil, ext4.wrapError(op, name, xerrors.Errorf("failed to read dir info: %w", err))
		}
		return info, nil
	}
	info, err := f.Stat()
	if err != nil {
		return nil, xerrors.Errorf("failed to stat file: %w", err)
	}
	return info, nil
}

func (ext4 *FileSystem) ReadDirInfo(name string) (fs.FileInfo, error) {
	if name == "/" {
		inode, err := ext4.getInode(rootInodeNumber)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse root inode: %w", err)
		}
		return FileInfo{
			name:  "/",
			inode: inode,
		}, nil
	}
	name = strings.TrimRight(name, "/")
	dirs, dir := path.Split(name)
	dirEntries, err := ext4.readDirEntry(dirs)
	if err != nil {
		return nil, xerrors.Errorf("failed to read dir entry: %w", err)
	}
	for _, entry := range dirEntries {
		if entry.Name() == strings.Trim(dir, "/") {
			return entry.Info()
		}
	}
	return nil, fs.ErrNotExist
}

// maxSymlinkDepth is the maximum number of symlink resolutions allowed
// before returning an error, to prevent infinite loops.
// This matches the Linux kernel's MAXSYMLINKS.
const maxSymlinkDepth = 40

func (ext4 *FileSystem) Open(name string) (fs.File, error) {
	return ext4.openWithDepth(name, 0)
}

func (ext4 *FileSystem) openWithDepth(name string, symlinkDepth int) (fs.File, error) {
	const op = "open"

	name = strings.TrimPrefix(name, "/")
	if !fs.ValidPath(name) {
		return nil, ext4.wrapError(op, name, fs.ErrInvalid)
	}

	dirName, fileName := filepath.Split(name)
	entries, err := ext4.ReadDir(dirName)
	if err != nil {
		return nil, ext4.wrapError(op, name, xerrors.Errorf("failed to read directory: %w", err))
	}

	for _, entry := range entries {
		if entry.IsDir() || entry.Name() != fileName {
			continue
		}
		dir, ok := entry.(dirEntry)
		if !ok {
			return nil, xerrors.Errorf("unspecified error, entry is not dir entry %+v", entry)
		}

		// Resolve symlinks
		if dir.inode.IsSymlink() {
			if symlinkDepth >= maxSymlinkDepth {
				return nil, ext4.wrapError(op, name, xerrors.New("too many levels of symbolic links"))
			}
			link, err := ext4.ReadLink(name)
			if err != nil {
				return nil, xerrors.Errorf("failed to read link: %w", err)
			}
			if !path.IsAbs(link) {
				link = path.Join(dirName, link)
			}
			return ext4.openWithDepth(link, symlinkDepth+1)
		}

		fi := FileInfo{
			name:  fileName,
			ino:   dir.ino,
			inode: dir.inode,
		}
		var f *File
		if fi.inode.UsesExtents() {
			f, err = ext4.file(fi, name)
		} else {
			f, err = ext4.fileFromBlock(fi, name)
		}
		if err != nil {
			return nil, xerrors.Errorf("failed to get file(inode: %d): %w", dir.ino, err)
		}
		return f, nil
	}
	return nil, fs.ErrNotExist
}

func (ext4 *FileSystem) ReadLink(name string) (string, error) {
	di, err := ext4.ReadDirInfo(name)
	if err != nil {
		return "", xerrors.Errorf("failed to read dir info: %w", err)
	}
	fi, ok := di.(FileInfo)
	if !ok {
		return "", xerrors.Errorf("unspecified error, entry is not file info %+v", fi)
	}
	inode := fi.inode
	if !inode.IsSymlink() {
		return "", xerrors.Errorf("file is not symlink: %w", fs.ErrInvalid)
	}

	// Depending on the target size, it is stored either in the inode block or the extents
	targetSize := inode.GetSize()
	if !inode.UsesExtents() {
		path := string(inode.BlockOrExtents[:targetSize])
		return filepath.Clean(path), nil
	}

	// For symlinks stored in extents, read the target using the File abstraction
	f, err := ext4.file(fi, name)
	if err != nil {
		return "", xerrors.Errorf("failed to create file reader: %w", err)
	}
	defer f.Close()

	target, err := io.ReadAll(f)
	if err != nil {
		return "", xerrors.Errorf("failed to read symlink target: %w", err)
	}

	return filepath.Clean(string(target)), nil
}

func (ext4 *FileSystem) Lstat(name string) (fs.FileInfo, error) {
	return ext4.Stat(name)
}

func (ext4 *FileSystem) fileFromBlock(fi FileInfo, filePath string) (*File, error) {
	blockAddresses, err := fi.inode.GetBlockAddresses(ext4)
	if err != nil {
		return nil, xerrors.Errorf("failed to get block addresses: %w", err)
	}

	dt := make(dataTable)
	for i, blockAddress := range blockAddresses {
		if blockAddress == 0 {
			continue
		}
		dt[int64(i)] = int64(blockAddress) * ext4.sb.GetBlockSize()
	}

	return &File{
		fs:           ext4,
		FileInfo:     fi,
		currentBlock: -1,
		buffer:       bytes.NewBuffer(nil),
		filePath:     filePath,
		blockSize:    ext4.sb.GetBlockSize(),
		table:        dt,
		size:         fi.Size(),
	}, nil
}

func (ext4 *FileSystem) file(fi FileInfo, filePath string) (*File, error) {
	extents, err := ext4.Extents(fi.inode)
	if err != nil {
		return nil, err
	}

	dt := make(dataTable)
	for _, e := range extents {
		// Uninitialized (unwritten) extents should read as zeros;
		// omitting them from the table delegates to the sparse path in Read().
		if e.IsUninitialized() {
			continue
		}
		offset := e.offset() * ext4.sb.GetBlockSize()
		for i := int64(0); i < int64(e.GetLen()); i++ {
			dt[int64(e.Block)+i] = offset + i*ext4.sb.GetBlockSize()
		}
	}

	return &File{
		fs:           ext4,
		FileInfo:     fi,
		currentBlock: -1,
		buffer:       bytes.NewBuffer(nil),
		filePath:     filePath,
		blockSize:    ext4.sb.GetBlockSize(),
		table:        dt,
		size:         fi.Size(),
	}, nil
}

func (ext4 *FileSystem) wrapError(op, path string, err error) error {
	return &fs.PathError{
		Op:   op,
		Path: path,
		Err:  err,
	}
}

func (ext4 *FileSystem) GetSuperBlock() Superblock {
	return ext4.sb
}
