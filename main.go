package main

import (
	"context"
	"fmt"
	"learn-be/db/lsm"
	"os"
)

func main() {
	logFile := "data/data.log"

	db := lsm.NewLSM(&lsm.Config{
		FileOutPath: logFile,
	})

	_ = os.Remove(logFile)

	err := db.Set(context.Background(), "key1", "value1")
	if err != nil {
		panic(err)
	}

	err = db.Set(context.Background(), "key2", "value2")
	if err != nil {
		panic(err)
	}

	err = db.Set(context.Background(), "key1", "value1-1")
	if err != nil {
		panic(err)
	}

	value, err := db.Get(context.Background(), "key1")
	if err != nil {
		panic(err)
	}

	fmt.Println(value)
}
