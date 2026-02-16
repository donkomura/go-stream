package main

import (
	"bufio"
	"fmt"
	"io"
	"iter"
	"os"
	"sync"
	"strings"
)

// FileLineStream provides a lazy line sequence over multiple files.
// Files are opened and consumed one by one, so it can handle inputs
// that do not fit in memory.
type FileLineStream struct {
	Seq iter.Seq[string]
	Err func() error
}

func trimLineEnding(line string) string {
	trimmed := strings.TrimSuffix(line, "\n")
	return strings.TrimSuffix(trimmed, "\r")
}

// NewFileLineStream creates a lazy line stream that reads files in order.
// The first I/O error is captured and can be retrieved via Err() after
// consuming the sequence.
func NewFileLineStream(paths []string) FileLineStream {
	var mu sync.RWMutex
	var lastErr error

	setLastErr := func(err error) {
		mu.Lock()
		lastErr = err
		mu.Unlock()
	}

	seq := func(yield func(string) bool) {
		var runErr error
		recordFirstRunErr := func(err error) {
			if err != nil && runErr == nil {
				runErr = err
			}
		}
		defer func() {
			setLastErr(runErr)
		}()

		streamFile := func(path string) (consumerStopped bool) {
			file, err := os.Open(path)
			if err != nil {
				recordFirstRunErr(fmt.Errorf("open %s: %w", path, err))
				return false
			}
			defer func() {
				if closeErr := file.Close(); closeErr != nil {
					recordFirstRunErr(fmt.Errorf("close %s: %w", path, closeErr))
				}
			}()

			reader := bufio.NewReader(file)
			for {
				line, readErr := reader.ReadString('\n')
				if len(line) > 0 {
					if !yield(trimLineEnding(line)) {
						return true
					}
				}

				if readErr == io.EOF {
					return false
				}
				if readErr != nil {
					recordFirstRunErr(fmt.Errorf("read %s: %w", path, readErr))
					return false
				}
			}
		}

		for _, path := range paths {
			if streamFile(path) {
				return
			}
			if runErr != nil {
				return
			}
		}
	}

	return FileLineStream{
		Seq: seq,
		Err: func() error {
			mu.RLock()
			defer mu.RUnlock()
			return lastErr
		},
	}
}
