package lsm

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
)

var delimiter = ","

type LSM struct {
	config *Config
}

type Config struct {
	FileOutPath string
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
	f, err := os.OpenFile(l.config.FileOutPath,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteString(fmt.Sprintf("%s%s%v\n", key, delimiter, value)); err != nil {
		return err
	}

	return nil
}

type ErrKeyNotFound struct {
	key string
}

func (e ErrKeyNotFound) Error() string {
	return fmt.Sprintf("key %s not found", e.key)
}

func (l *LSM) Get(ctx context.Context, key string) (any, error) {
	readFile, err := os.Open(l.config.FileOutPath)
	if err != nil {
		return nil, err
	}
	defer readFile.Close()

	fileScanner := bufio.NewScanner(readFile)

	var value any
	var found bool
	for fileScanner.Scan() {
		line := fileScanner.Text()
		pKey, pValue := l.parseLine(line)
		if pKey == key {
			value = pValue
			found = true
		}
	}

	if !found {
		return nil, ErrKeyNotFound{key}
	}

	return value, nil
}

func (*LSM) parseLine(line string) (key string, value any) {
	parts := strings.Split(line, delimiter)
	return parts[0], parts[1]
}
