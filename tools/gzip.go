package tools

import (
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func sha256sum(dst string, sum *bufio.Writer) error {
	f, err := os.Open(dst)
	if err != nil {
		return err
	}

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	fmt.Fprintf(sum, "%s\t%x\n", filepath.Base(dst), h.Sum(nil))
	f.Close()
	return nil
}

func copyCompress(src *bufio.Reader, sum *bufio.Writer, dst string) error {
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	zipout := gzip.NewWriter(out)
	// Setting the Header fields is optional.
	zipout.Name = dst
	zipout.Comment = "The OSCAR corpus was put together by Pedro J. Ortiz, Benoît Sagot, and Laurent Romary."
	zipout.ModTime = time.Now()

	_, err = io.Copy(zipout, src)
	if err != nil {
		return err
	}
	zipout.Flush()
	zipout.Close()
	out.Close()

	err = sha256sum(dst, sum)
	sum.Flush()
	return err
}

func copyNcompress(src *bufio.Reader, sum *bufio.Writer, dst string, chunkSize int64) error {
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	zipout := gzip.NewWriter(out)
	// Setting the Header fields is optional.
	zipout.Name = dst
	zipout.Comment = "The OSCAR corpus was put together by Pedro J. Ortiz, Benoît Sagot, and Laurent Romary."
	zipout.ModTime = time.Now()

	_, err = io.CopyN(zipout, src, chunkSize)
	if err != nil {
		if err == io.EOF {
			zipout.Flush()
			zipout.Close()
			out.Close()

			if err2 := sha256sum(dst, sum); err2 != nil {
				return err2
			}

			return err
		}
		return err
	}
	doc := 0
	for par, err := src.ReadBytes('\n'); err == nil && doc < 2; par, err = src.ReadBytes('\n') {
		zipout.Write(par)
		if string(par) == "\n" {
			doc++
		}
	}

	if err != nil {
		if err == io.EOF {
			zipout.Flush()
			zipout.Close()
			out.Close()

			if err2 := sha256sum(dst, sum); err2 != nil {
				return err2
			}

			return err
		}
		return err
	}

	zipout.Flush()
	zipout.Close()
	out.Close()
	if err2 := sha256sum(dst, sum); err2 != nil {
		return err2
	}
	return nil
}

// Extract takes a gzip and extracts its contents
// Taken and adapted from https://github.com/ChrisCates/CommonCrawler
// MIT License, Copyright (c) 2017 Chris Cates
func Extract(path string, data chan<- string) error {
	//get extracted file path
	fname := filepath.Base(path)
	ext := filepath.Ext(fname)
	fileName := fname[:len(fname)-len(ext)]
	//create extruction destination

	var extractedPath strings.Builder
	extractedPath.WriteString("data/")
	extractedPath.WriteString(fileName)

	out, err := os.Create(extractedPath.String())
	if err != nil {

		return err
	}
	defer out.Close()

	//open gzip file
	fi, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fi.Close()
	//create gz reader
	fz, err := gzip.NewReader(fi)
	if err != nil {
		return err
	}
	defer fz.Close()

	//write extracted to file
	_, err = io.Copy(out, fz)
	if err != nil {
		return err
	}

	data <- extractedPath.String()

	return nil
}
