package ext4

import (
	"io/fs"
	"path/filepath"
	"time"
)

// type FileInfo interface {
//	 	Name() string       // base name of the file
//	 	Size() int64        // length in bytes for regular files; system-dependent for others
//	 	Mode() FileMode     // file mode bits
//	 	ModTime() time.Time // modification time
//	 	IsDir() bool        // abbreviation for Mode().IsDir()
//	 	Sys() interface{}   // underlying data source (can return nil)
// }
var _ fs.File = File{}
var _ fs.FileInfo = FileInfo{}

// File is implemented io/fs File interface
type File struct {
	filePath string
	mTime    uint32
	mode     uint16
	size     int64
	isDir    bool

	fs *FileSystem
	FileInfo
}

// FileInfo is implemented io/fs FileInfo interface
type FileInfo struct {
	name  string
	inode *Inode

	mode fs.FileMode
}

func (f File) Name() string {
	_, name := filepath.Split(f.filePath)
	return name
}

func (f File) Dir() string {
	dir, _ := filepath.Split(f.filePath)
	return dir
}

func (f File) FilePath() string {
	return f.filePath
}

func (f File) Size() int64 {
	return f.size
}

func (f File) Mode() fs.FileMode {
	return fs.FileMode(f.mode)
}

func (f File) ModTime() time.Time {
	return time.Unix(int64(f.mTime), 0)
}

func (f File) IsDir() bool {
	return f.isDir
}

func (f File) Sys() interface{} {
	return nil
}

func (fi FileInfo) Name() string {
	return fi.name
}

func (fi FileInfo) Size() int64 {
	//TODO implement me
	panic("implement me")
}

func (fi FileInfo) Mode() fs.FileMode {
	//TODO implement me
	panic("implement me")
}

func (fi FileInfo) ModTime() time.Time {
	//TODO implement me
	panic("implement me")
}

func (fi FileInfo) IsDir() bool {
	//TODO implement me
	panic("implement me")
}

func (fi FileInfo) Sys() interface{} {
	//TODO implement me
	panic("implement me")
}

func (f File) Stat() (fs.FileInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (f File) Read(bytes []byte) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (f File) Close() error {
	//TODO implement me
	panic("implement me")
}
