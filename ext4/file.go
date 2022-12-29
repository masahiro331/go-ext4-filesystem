package ext4

import (
	"bytes"
	"io"
	"io/fs"
	"path/filepath"
	"time"

	"golang.org/x/xerrors"
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

	mode fs.FileMode
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
	return fi.mode
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
		// blockSize: 512
		// size: 2000
		// 2000 - 512 * 3 = 464 < 512
		if f.Size()-f.blockSize*f.currentBlock < f.blockSize {
			f.buffer.Write(make([]byte, f.Size()-f.blockSize*f.currentBlock))
		}
		f.buffer.Write(make([]byte, f.blockSize))
	} else {
		buf, err := readBlockAt(f.fs.r, offset, f.blockSize)
		if err != nil {
			return 0, xerrors.Errorf("failed to read block: %w", err)
		}

		if f.Size()-f.blockSize*f.currentBlock < f.blockSize {
			buf = buf[:f.Size()-f.blockSize*f.currentBlock]
		}
		n, err := f.buffer.Write(buf)
		if n != len(buf) {
			return 0, xerrors.Errorf("write buffer error: actual(%d), expected(%d)", n, len(buf))
		}
	}

	return f.buffer.Read(p)
}

func (f *File) Close() error {
	return nil
}
