package lsm

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

var (
	delimiter    = ","
	baseFileName = "data"
	fileExt      = "log"
	fileName     = func(id string) string {
		return fmt.Sprintf("%s_%s.%s", baseFileName, id, fileExt)
	}
	fileID = func(fileName string) string {
		parts := strings.Split(fileName, ".")
		name, _ := parts[0], parts[1]

		nameParts := strings.Split(name, "_")
		return nameParts[len(nameParts)-1]
	}
)

type LSM struct {
	config *Config
}

type Config struct {
	FileOutDir      string
	SegmentMaxLines int
}

func NewLSM(c *Config) *LSM {
	return &LSM{
		config: c,
	}
}

func (l *LSM) Set(ctx context.Context, key string, value any) error {
	return l.append(ctx, key, value)
}

func (l *LSM) append(ctx context.Context, key string, value any) error {
	files := l.listFiles()

	var latestFile string
	if len(files) == 0 {
		latestFile = fileName("0")
	} else {
		latestFile = files[0]
	}

	f, err := os.OpenFile(filepath.Join(l.config.FileOutDir, latestFile),
		os.O_APPEND|os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return err
	}
	defer f.Close()

	numLines, err := l.countLines(f)
	if err != nil {
		return err
	}

	if _, err := f.WriteString(fmt.Sprintf("%s%s%v\n", key, delimiter, value)); err != nil {
		return err
	}

	if numLines == l.config.SegmentMaxLines-1 {
		id, _ := strconv.Atoi(fileID(latestFile))
		newF, err := os.Create(filepath.Join(l.config.FileOutDir, fileName(strconv.Itoa(id+1))))
		if err != nil {
			return err
		}
		_ = newF.Close()
	}

	return nil
}

func (l *LSM) countLines(r io.Reader) (int, error) {
	buf := make([]byte, 32*1024)
	lineSep := []byte{'\n'}
	count := 0

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil
		case err != nil:
			return count, err
		}
	}
}

type ErrKeyNotFound struct {
	key string
}

func (e ErrKeyNotFound) Error() string {
	return fmt.Sprintf("key %s not found", e.key)
}

func (l *LSM) Get(ctx context.Context, key string) (any, error) {

	files := l.listFiles()

	var value any
	var found bool

	for _, fName := range files {
		readFile, err := os.Open(filepath.Join(l.config.FileOutDir, fName))
		if err != nil {
			return nil, err
		}

		fileScanner := bufio.NewScanner(readFile)

		for fileScanner.Scan() {
			line := fileScanner.Text()
			pKey, pValue := l.parseLine(line)
			if pKey == key {
				value = pValue
				found = true
			}
		}

		_ = readFile.Close()

		if found {
			break
		}
	}

	if !found {
		return nil, ErrKeyNotFound{key}
	}

	return value, nil
}

func (l *LSM) listFiles() []string {
	entries, err := os.ReadDir(l.config.FileOutDir)
	if errors.Is(err, syscall.ENOENT) {
		_ = os.Mkdir(l.config.FileOutDir, 0777)
	}

	var files []string
	for i := len(entries) - 1; i >= 0; i-- {
		files = append(files, entries[i].Name())
	}

	return files
}

func (*LSM) parseLine(line string) (key string, value any) {
	parts := strings.Split(line, delimiter)
	return parts[0], parts[1]
}
