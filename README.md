# go-ext4-filesystem 

go-ext4-filesystem is ext4 filesystem stream parser.  
This library read stream from filesystem.


## Quick Start
```
func main() {
    f, err := os.Open("filesystem.ext4")
    if err != nil {
        log.Fatal(err)
    }

    reader, err := ext4.NewReader(f)
    if err != nil {
        log.Fatal(err)
    }

    for {
        fileName, err := reader.Next()
        if err != nil {
            if err == io.EOF {
                 break
             }
            log.Fatal(err)
        }
        if fileName == "/etc/passwd" {
            b, err := ioutil.ReadAll(reader)
            if err != nil {
                log.Fatal(err)
            }
            fmt.Println(b)
            break
        }
    }

}

```
