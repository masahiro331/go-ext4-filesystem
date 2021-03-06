package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	ext4 "github.com/masahiro331/go-ext4-filesystem/pkg"
)

func main() {
	f, err := os.Open("./testdata/1.img")
	if err != nil {
		log.Fatal(err)
	}

	reader, err := ext4.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}

	buf := make([]byte, 1024)
	for {
		file, err := reader.Next()
		if err != nil {
			log.Fatal(err)
		}

		if strings.Contains(file.Name(), "etc/os-release") {
			_, err := reader.Read(buf)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(string(buf))
		}
	}
}
