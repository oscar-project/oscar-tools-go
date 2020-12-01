package tools

import (
	"bufio"

	"github.com/cespare/xxhash"
)

// Dedup Takes a textfile and deduplicates it on a line basis usinf the xxhash algorithm
func Dedup() {
	bufin := bufio.NewReader(in)
	bufout := bufio.NewWriter(out)

	tab := make(map[uint64]int)

	for par, err := bufin.ReadString('\n'); err == nil; par, err = bufin.ReadString('\n') {
		hash := xxhash.Sum64String(par)
		if _, ok := tab[hash]; !ok || par == "\n" {
			tab[hash] = 1
			bufout.WriteString(par)
		}
	}
	bufout.Flush()
	in.Close()
	out.Close()
}
