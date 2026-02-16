package main

import (
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	// ETL example: input from multiple files (streamed, not fully loaded).
	dir, err := os.MkdirTemp("", "go-stream-etl-example")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	file1 := filepath.Join(dir, "events-1.log")
	file2 := filepath.Join(dir, "events-2.log")
	if err := os.WriteFile(file1, []byte("apple\nbanana\napple\n"), 0o644); err != nil {
		panic(err)
	}
	if err := os.WriteFile(file2, []byte("orange\napple\nbanana\n"), 0o644); err != nil {
		panic(err)
	}

	source := NewFileLineStream([]string{file1, file2})
	appleCount := Stream(
		source.Seq,
		Filter(func(v string) bool { return v == "apple" },
			End(Count[string]()),
		),
	)
	if err := source.Err(); err != nil {
		panic(err)
	}
	fmt.Printf("etl (files -> filter -> aggregate): apple count=%d\n", appleCount)
}
