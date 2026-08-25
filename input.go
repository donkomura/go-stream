package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
)

type runErrState struct {
	mu   sync.RWMutex
	last error
}

func (s *runErrState) Set(err error) {
	s.mu.Lock()
	s.last = err
	s.mu.Unlock()
}

func (s *runErrState) Get() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.last
}

func setFirstErr(dst *error, err error) {
	if err != nil && *dst == nil {
		*dst = err
	}
}

// FileInput is the interface passed from file stream to parsing.
// It abstracts how a file is opened, so parsing can focus on decoding logic.
type FileInput interface {
	Path() string
	Open() (io.ReadCloser, error)
}

type localFileInput struct {
	path string
}

func (f localFileInput) Path() string {
	return f.path
}

func (f localFileInput) Open() (io.ReadCloser, error) {
	return os.Open(f.path)
}

// FileParser abstracts parser implementations for any file format.
type FileParser[T any] interface {
	Parse(path string, r io.Reader, yield func(T) bool) error
}

func trimLineEnding(line string) string {
	trimmed := strings.TrimSuffix(line, "\n")
	return strings.TrimSuffix(trimmed, "\r")
}

// NewFileStream creates a lazy file reference stream in path order.
// It validates each file exists before yielding it.
func NewFileStream(paths []string) Stream[FileInput] {
	var state runErrState

	seq := func(yield func(FileInput) bool) {
		var runErr error
		defer func() {
			state.Set(runErr)
		}()

		for _, path := range paths {
			if _, err := os.Stat(path); err != nil {
				setFirstErr(&runErr, fmt.Errorf("stat %s: %w", path, err))
				return
			}

			if !yield(localFileInput{path: path}) {
				return
			}
		}
	}

	return Stream[FileInput]{seq: seq, err: state.Get}
}

// ParseFiles creates a parsed input stream by connecting a file stream and a
// FileParser. This is the boundary between file streaming and format parsing.
func ParseFiles[T any](files Stream[FileInput], parser FileParser[T]) Stream[T] {
	var state runErrState

	seq := func(yield func(T) bool) {
		var runErr error
		defer func() {
			state.Set(runErr)
		}()

		for file := range files.Seq() {
			consumerStopped, err := parseFileWith[T](file, parser, yield)
			setFirstErr(&runErr, err)
			if consumerStopped {
				return
			}
			if runErr != nil {
				return
			}
		}
		setFirstErr(&runErr, files.Err())
	}

	return Stream[T]{seq: seq, err: state.Get}
}

func parseFileWith[T any](file FileInput, parser FileParser[T], yield func(T) bool) (consumerStopped bool, err error) {
	reader, openErr := file.Open()
	if openErr != nil {
		return false, fmt.Errorf("open %s: %w", file.Path(), openErr)
	}

	stopped := false
	parseErr := parser.Parse(file.Path(), reader, func(v T) bool {
		if !yield(v) {
			stopped = true
			return false
		}
		return true
	})
	if parseErr != nil {
		setFirstErr(&err, fmt.Errorf("parse %s: %w", file.Path(), parseErr))
	}
	if closeErr := reader.Close(); closeErr != nil {
		setFirstErr(&err, fmt.Errorf("close %s: %w", file.Path(), closeErr))
	}

	return stopped, err
}

// LineParser parses text files into line records.
type LineParser struct{}

func (LineParser) Parse(_ string, r io.Reader, yield func(string) bool) error {
	reader := bufio.NewReader(r)
	for {
		line, readErr := reader.ReadString('\n')
		if len(line) > 0 {
			if !yield(trimLineEnding(line)) {
				return nil
			}
		}

		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

// CSVParser parses CSV files and yields each record as []string.
type CSVParser struct {
	Comma            rune
	Comment          rune
	TrimLeadingSpace bool
	FieldsPerRecord  int
	LazyQuotes       bool
}

func (p CSVParser) Parse(_ string, r io.Reader, yield func([]string) bool) error {
	reader := csv.NewReader(r)
	if p.Comma != 0 {
		reader.Comma = p.Comma
	}
	if p.Comment != 0 {
		reader.Comment = p.Comment
	}
	reader.TrimLeadingSpace = p.TrimLeadingSpace
	reader.FieldsPerRecord = p.FieldsPerRecord
	reader.LazyQuotes = p.LazyQuotes

	for {
		record, err := reader.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		// csv.Reader reuses its record buffer between calls.
		if !yield(slices.Clone(record)) {
			return nil
		}
	}
}

// NewFileLineStream composes FileStream -> LineParser into a line stream.
func NewFileLineStream(paths []string) Stream[string] {
	return ParseFiles[string](NewFileStream(paths), LineParser{})
}

// NewFileCSVStream composes FileStream -> CSVParser into a record stream.
func NewFileCSVStream(paths []string) Stream[[]string] {
	return ParseFiles[[]string](NewFileStream(paths), CSVParser{})
}
