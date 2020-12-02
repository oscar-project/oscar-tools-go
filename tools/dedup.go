package tools

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"

	"github.com/cespare/xxhash"
)

// Dedup Takes a textfile and deduplicates it on a line basis usinf the xxhash algorithm
func Dedup(path string, dest string) error {
	in, err := os.Open(path)
	if err != nil {
		return err
	}
	basename := filepath.Base(path)
	name := strings.TrimSuffix(basename, filepath.Ext(basename))
	out, err := os.Create(filepath.Join(dest, name+"_dedup"+filepath.Ext(basename)))
	if err != nil {
		return err
	}

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
	return out.Close()
}
