# go-ext4-filesystem 

go-ext4-filesystem is ext4 filesystem stream parser.  
This library implement io/fs interface. 


## Quick Start
```
func main() {
    f, err := os.Open("filesystem.ext4")
    if err != nil {
        log.Fatal(err)
    }
	info, _ := f.Stat()
    filesystem, err := ext4.NewFS(io.NewSectionReader(f,0, info.Size()))
    if err != nil {
        log.Fatal(err)
    }
    
	fs.WalkDir(filesystem, "/", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return xerrors.Errorf("file walk error: %w", err)
		}
		if d.IsDir() {
			return nil
		}

		fmt.Println(path)
		if path == "/usr/lib/os-release" {
			of, _ := os.Create("os-release")
			defer of.Close()

			sf, err := filesystem.Open(path)
			if err != nil {
				return err
			}
			io.Copy(of, sf)
		}
		return nil
	})
}

```
