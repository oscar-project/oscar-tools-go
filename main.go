package main

import (
	"log"
	"os"
	"sync"

	"github.com/pjox/oscar-tools/tools"
)

func main() {

	done := make(chan struct{})
	defer close(done)

	paths, errc := tools.WalkFiles(done, os.Args[1])

	var wg sync.WaitGroup
	maxGoroutines := 10
	guard := make(chan struct{}, maxGoroutines)

	for path := range paths {
		wg.Add(1)
		go func(path string) {
			guard <- struct{}{}
			err := tools.Dedup(path, os.Args[2]) // HLc
			if err != nil {
				log.Fatalln(err)
			}
			<-guard
			wg.Done()
		}(path)
	}

	// Check whether the Walk failed.
	if err := <-errc; err != nil { // HLerrc
		log.Fatal(err)
	}
	wg.Wait()
}
