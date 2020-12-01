package tools

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
)

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
