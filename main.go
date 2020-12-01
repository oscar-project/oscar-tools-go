package main

import (
	"fmt"
	"os"

	"github.com/pjox/oscar-tools/tools"
)

func main() {

	done := make(chan struct{})
	defer close(done)

	paths, errc := tools.WalkFiles(done, os.Args[1])

	fmt.Println("Hello")

	in, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Println(err)
	}
	out, err := os.Create(os.Args[2])
	if err != nil {
		fmt.Println(err)
	}

}
