package tools

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func copy(src *bufio.Reader, dst string) error {
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	bufout := bufio.NewWriter(out)
	_, err = io.Copy(bufout, src)
	if err != nil {
		return err
	}
	bufout.Flush()
	return out.Close()
}

func copyN(src *bufio.Reader, dst string, chunkSize int64) (int64, error) {
	out, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	bufout := bufio.NewWriter(out)
	written, err := io.CopyN(bufout, src, chunkSize)
	if err != nil {
		if err == io.EOF {
			return written, err
		}
		return 0, err
	}
	doc := 0
	for par, err := src.ReadString('\n'); err == nil && doc < 2; par, err = src.ReadString('\n') {
		bufout.WriteString(par)
		if par == "\n" {
			doc++
		}
	}
	bufout.Flush()
	return written, out.Close()
}

// Split splits plaintext files into a files of a given size
func Split(path string, dest string, chunkSize int64) error {
	in, err := os.Open(path)
	if err != nil {
		return err
	}
	bufin := bufio.NewReader(in)

	basename := filepath.Base(path)
	name := strings.TrimSuffix(basename, filepath.Ext(basename))
	if strings.HasSuffix(name, "_dedup") {
		name = strings.TrimSuffix(name, "_dedup")
	}
	folderName := filepath.Join(dest, name)
	err = os.Mkdir(folderName, 0755)
	if err != nil {
		return err
	}

	fileInfo, err := in.Stat()
	if err != nil {
		return err
	}

	if fileInfo.Size() <= chunkSize {
		filePath := filepath.Join(folderName, fileInfo.Name())
		return copy(bufin, filePath)
	}

	fileCounter := 1
	var written int64
	for written = 0; written < fileInfo.Size(); fileCounter++ {
		var filePathBuff strings.Builder
		filePathBuff.WriteString(name)
		filePathBuff.WriteString("_part_")
		filePathBuff.WriteString(strconv.Itoa(fileCounter))
		filePathBuff.WriteString(filepath.Ext(basename))
		filePath := filepath.Join(folderName, filePathBuff.String())

		auxWritten, err := copyN(bufin, filePath, chunkSize)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		written += auxWritten
	}

	return nil
}
