package main

import (
	"log"
	"os"

	ext4 "github.com/masahiro331/go-ext4-filesystem/pkg"
)

func main() {
	f, err := os.Open("./testdata/1.img")
	if err != nil {
		log.Fatal(err)
	}

	_, err = ext4.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}
}
