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

func copyN(src *bufio.Reader, dst string, chunkSize int64) error {
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	bufout := bufio.NewWriter(out)
	_, err = io.CopyN(bufout, src, chunkSize)
	if err != nil {
		if err == io.EOF {
			bufout.Flush()
			out.Close()
			return err
		}
		return err
	}
	doc := 0
	for par, err := src.ReadString('\n'); err == nil && doc < 2; par, err = src.ReadString('\n') {
		bufout.WriteString(par)
		if par == "\n" {
			doc++
		}
	}
	if err != nil {
		if err == io.EOF {
			bufout.Flush()
			out.Close()
			return err
		}
		return err
	}
	bufout.Flush()
	return out.Close()
}

// Split splits plaintext files into a files of a given size
func Split(path string, dest string, chunkSize int64, compress bool) error {
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

	var sumPath strings.Builder
	sumPath.WriteString(folderName)
	sumPath.WriteString("/")
	sumPath.WriteString(name)
	sumPath.WriteString("_sha256")
	sumPath.WriteString(".txt")
	sumFile, err := os.OpenFile(sumPath.String(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	sum := bufio.NewWriter(sumFile)
	dsts := make(chan string, 50)

	errc := sha256sum(dsts, sum)

	fileInfo, err := in.Stat()
	if err != nil {
		return err
	}

	if fileInfo.Size() <= chunkSize {
		filePath := filepath.Join(folderName, fileInfo.Name())
		if compress {
			filePath = filePath + ".gz"
			return copyCompress(bufin, dsts, filePath)
		}
		return copy(bufin, filePath)
	}

	fileCounter := 1
	for {
		var filePathBuff strings.Builder
		filePathBuff.WriteString(name)
		filePathBuff.WriteString("_part_")
		filePathBuff.WriteString(strconv.Itoa(fileCounter))
		filePathBuff.WriteString(filepath.Ext(basename))
		if compress {
			filePathBuff.WriteString(".gz")
		}
		filePath := filepath.Join(folderName, filePathBuff.String())

		if compress {
			err = copyNcompress(bufin, dsts, filePath, chunkSize)
		} else {
			err = copyN(bufin, filePath, chunkSize)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		fileCounter++
	}

	close(dsts)

	if err := <-errc; err != nil { // HLerrc
		return err
	}

	in.Close()
	sum.Flush()
	sumFile.Close()
	return nil
}
