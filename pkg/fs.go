package ext4

import (
	"encoding/binary"
	"fmt"
	"github.com/lunixbochs/struc"
	"golang.org/x/xerrors"
	"io"
	"io/fs"
)

var (
	_ fs.FS        = &FileSystem{}
	_ fs.ReadDirFS = &FileSystem{}
	_ fs.StatFS    = &FileSystem{}

	_ fs.File     = &File{}
	_ fs.FileInfo = &FileInfo{}
	_ fs.DirEntry = dirEntry{}
)

// FileSystem is implemented io/fs interface
type FileSystem struct {
	r *io.SectionReader

	sb  Superblock
	gds []GroupDescriptor
}

// NewFS is created io/fs.FS for ext4 filesystem
func NewFS(r io.SectionReader, sectorSize int64) (*FileSystem, error) {
	_, err := r.Seek(GroupZeroPadding, 0)
	if err != nil {
		return nil, xerrors.Errorf("failed to seek padding: %w", err)
	}
	buf, err := readBlock(&r, SuperBlockSize)
	if err != nil {
		return nil, xerrors.Errorf("failed to read super block: %w", err)
	}
	var sb Superblock
	if err := binary.Read(buf, binary.LittleEndian, &sb); err != nil {
		return nil, xerrors.Errorf("failed to binary read super block: %w", err)
	}
	if sb.Magic != 0xEF53 {
		return nil, xerrors.New("unsupported block")
	}

	numBlockGroups := (sb.GetBlockCount() + int64(sb.BlockPerGroup) - 1) / int64(sb.BlockPerGroup)
	numBlockGroups2 := (sb.InodeCount + sb.InodePerGroup - 1) / sb.InodePerGroup
	if numBlockGroups != int64(numBlockGroups2) {
		return nil, xerrors.Errorf("Block/inode mismatch: %d %d %d", sb.GetBlockCount(), numBlockGroups, numBlockGroups2)
	}

	gds, err := sb.getGroupDescriptor(r)
	if err != nil {
		return nil, xerrors.Errorf("failed to get group Descriptor: %w", err)
	}

	fs := &FileSystem{
		r:   &r,
		sb:  sb,
		gds: gds,
	}

	return fs, nil
}

func (ext4 *FileSystem) ReadDir(path string) ([]fs.DirEntry, error) {
	const op = "read directory"

	// dirEntries, err := ext4.readDirEntry(path, nil)
	// if err != nil {
	// 	return nil, ext4.wrapError(op, path, err)
	// }
	// return dirEntries, nil

	ino := int64(rootInodeNumber)
	entries, err := ext4.walk(ino, nil)
	if err != nil {
		return nil, ext4.wrapError(op, path, err)
	}
	fmt.Println(len(entries))

	return nil, nil
}

var DIRECTORY_MODE = uint16(16877)

func (ext4 *FileSystem) walk(ino int64, entries []DirectoryEntry2) ([]DirectoryEntry2, error) {
	inode, err := ext4.getInode(ino)
	if err != nil {
		return nil, xerrors.Errorf("failed to get root inode(%d): %w", ext4.sb.FirstIno, err)
	}
	if inode.UsesDirectoryHashTree() {
	}

	if inode.Mode != DIRECTORY_MODE {
		return entries, nil
	}

	extents, err := ext4.Extents(inode)
	if err != nil {
		return nil, xerrors.Errorf("failed to get extents error: %w", err)
	}

	for _, e := range extents {
		_, err := ext4.r.Seek(e.offset()*ext4.sb.GetBlockSize(), 0)
		if err != nil {
			return nil, xerrors.Errorf("failed to seek: %w", err)
		}
		directoryReader, err := readBlock(ext4.r, ext4.sb.GetBlockSize()*int64(e.Len))
		if err != nil {
			return nil, xerrors.Errorf("failed to read directory entry: %w", err)
		}
		for {
			dirEntry := DirectoryEntry2{}
			err = struc.Unpack(directoryReader, &dirEntry)
			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, xerrors.Errorf("failed to parse directory entry: %w", err)
			}
			align := dirEntry.RecLen - uint16(dirEntry.NameLen+8)
			_, err := directoryReader.Read(make([]byte, align))
			if err != nil {
				return nil, xerrors.Errorf("failed to read align: %w", err)
			}
			if dirEntry.Name == "." || dirEntry.Name == ".." {
				continue
			}
			if dirEntry.Flags == 0xDE {
				break
			}
			entries = append(entries, dirEntry)
			entries, err = ext4.walk(int64(dirEntry.Inode), entries)
			if err != nil {
				return nil, xerrors.Errorf("failed to walk: %w", err)
			}
		}
	}
	return entries, nil
}

func (ext4 *FileSystem) readDirEntry(path string, dirEntry []fs.DirEntry) ([]fs.DirEntry, error) {
	return nil, nil
}

func (ext4 *FileSystem) ReadDirectory(inode Inode) ([]DirectoryEntry2, error) {
	return nil, nil
}

func (ext4 *FileSystem) Stat(name string) (fs.FileInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (ext4 *FileSystem) Open(name string) (fs.File, error) {
	//TODO implement me
	panic("implement me")
}

// Type dirEntry is implemented io/fs DirEntry interface
type dirEntry struct {
	FileInfo
}

func (d dirEntry) Type() fs.FileMode {
	return d.FileInfo.Mode().Type()
}

func (d dirEntry) Info() (fs.FileInfo, error) { return d.FileInfo, nil }

func (ext4 *FileSystem) wrapError(op, path string, err error) error {
	return &fs.PathError{
		Op:   op,
		Path: path,
		Err:  err,
	}
}
