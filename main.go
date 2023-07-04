package main

import (
	"context"
	"fmt"
	db2 "learn-be/db"
	"learn-be/db/lsm"
	"math/rand"
	"os"
)

func main() {
	logDir := "data"

	var db db2.DB
	db = lsm.NewLSM(&lsm.Config{
		FileOutDir:      logDir,
		SegmentMaxLines: 3,
	})

	_ = os.Remove(logDir)

	for i := 0; i <= 10; i++ {
		err := db.Set(context.Background(), "key1", rand.Intn(30))
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i <= 4; i++ {
		err := db.Set(context.Background(), "key2", rand.Intn(30))
		if err != nil {
			panic(err)
		}
	}

	value1, err := db.Get(context.Background(), "key1")
	if err != nil {
		panic(err)
	}

	value2, err := db.Get(context.Background(), "key2")
	if err != nil {
		panic(err)
	}

	fmt.Println(value1)
	fmt.Println(value2)
}
