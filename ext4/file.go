package ext4

import (
	"bytes"
	"golang.org/x/xerrors"
	"io"
	"io/fs"
	"path/filepath"
	"time"
)

var (
	_ fs.File     = &File{}
	_ fs.FileInfo = &FileInfo{}
	_ fs.DirEntry = dirEntry{}
)

// File is implemented io/fs File interface
type File struct {
	FileInfo
	fs           *FileSystem
	buffer       *bytes.Buffer
	filePath     string
	currentBlock int64
	size         int64
	blockSize    int64
	table        dataTable
}

type dataTable map[int64]int64

// FileInfo is implemented io/fs FileInfo interface
type FileInfo struct {
	name  string
	inode *Inode
	ino   int64
}

// Type dirEntry is implemented io/fs DirEntry interface
type dirEntry struct {
	FileInfo
}

func (d dirEntry) Type() fs.FileMode {
	return d.FileInfo.Mode().Type()
}

func (f FileInfo) IsSymlink() bool {
	return f.Mode()&fs.ModeSymlink != 0
}

func (d dirEntry) Info() (fs.FileInfo, error) { return d.FileInfo, nil }

func (f File) Dir() string {
	dir, _ := filepath.Split(f.filePath)
	return dir
}

func (f File) FilePath() string {
	return f.filePath
}

func (fi FileInfo) Name() string {
	return fi.name
}

func (fi FileInfo) Size() int64 {
	return fi.inode.GetSize()
}

func (fi FileInfo) Mode() fs.FileMode {
	m := fi.inode.Mode
	mode := fs.FileMode(m & 0o777)

	if m&0o1000 != 0 {
		mode |= fs.ModeSticky
	}
	if m&0o2000 != 0 {
		mode |= fs.ModeSetgid
	}
	if m&0o4000 != 0 {
		mode |= fs.ModeSetuid
	}

	switch m & 0xF000 {
	case 0xC000:
		mode |= fs.ModeSocket
	case 0xA000:
		mode |= fs.ModeSymlink
	case 0x8000:
		// regular file
	case 0x6000:
		mode |= fs.ModeDevice
	case 0x4000:
		mode |= fs.ModeDir
	case 0x2000:
		mode |= fs.ModeCharDevice
	case 0x1000:
		mode |= fs.ModeNamedPipe
	default:
		mode |= fs.ModeIrregular
	}

	return mode
}

func (fi FileInfo) ModTime() time.Time {
	return time.Unix(int64(fi.inode.Mtime), 0)
}

func (fi FileInfo) IsDir() bool {
	return fi.inode.IsDir()
}

func (fi FileInfo) Sys() interface{} {
	return nil
}

func (f *File) Stat() (fs.FileInfo, error) {
	return &f.FileInfo, nil
}

func (f *File) Read(p []byte) (n int, err error) {
	if f.buffer == nil {
		return 0, io.EOF
	}
	if f.buffer.Len() == 0 {
		f.currentBlock++
		if f.currentBlock*f.blockSize >= f.Size() {
			f.buffer = nil
			return 0, io.EOF
		}
	} else {
		return f.buffer.Read(p)
	}

	offset, ok := f.table[f.currentBlock]
	if !ok {
		remaining := f.Size() - f.blockSize*f.currentBlock
		if remaining < f.blockSize {
			f.buffer.Write(make([]byte, remaining))
		} else {
			f.buffer.Write(make([]byte, f.blockSize))
		}
	} else {
		_, err := f.fs.r.Seek(offset, io.SeekStart)
		if err != nil {
			return 0, xerrors.Errorf("failed to seek block: %w", err)
		}
		buf, err := readBlock(f.fs.r, f.blockSize)
		if err != nil {
			return 0, xerrors.Errorf("failed to read block: %w", err)
		}

		b := buf.Bytes()
		if f.Size()-f.blockSize*f.currentBlock < f.blockSize {
			b = b[:f.Size()-f.blockSize*f.currentBlock]
		}
		n, err := f.buffer.Write(b)
		if n != len(b) {
			return 0, xerrors.Errorf("write buffer error: actual(%d), expected(%d)", n, len(b))
		}
	}

	return f.buffer.Read(p)
}

func (f *File) Close() error {
	return nil
}
