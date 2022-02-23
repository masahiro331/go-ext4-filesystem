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
	// If implement io.Seeker, need to fs *Filesystem member
	r         io.Reader
	buf       *bytes.Buffer
	extents   []Extent
	filePath  string
	availsize int64

	FileInfo
}

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
	for {
		if len(p) > f.buf.Len() {
			_, err := io.CopyN(f.buf, f.r, SectorSize)
			if err == io.EOF {
				break
			}
			if err != nil {
				return 0, xerrors.Errorf("failed to read file from source: %w", err)
			}
		} else {
			break
		}
		return 0, nil
	}

	n, err = f.buf.Read(p)
	if err == io.EOF {
		return 0, io.EOF
	}
	if err != nil {
		return 0, xerrors.Errorf("failed to read file from buffer: %w", err)
	}
	f.availsize -= int64(n)
	if f.availsize < 0 {
		return n + int(f.availsize), err
	}
	return n, err
}

func (f *File) Close() error {
	return nil
}
