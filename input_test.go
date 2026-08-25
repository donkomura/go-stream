package main

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestNewFileLineStream(t *testing.T) {
	t.Run("reads multiple files sequentially", func(t *testing.T) {
		dir := t.TempDir()
		fileA := filepath.Join(dir, "a.txt")
		fileB := filepath.Join(dir, "b.txt")

		writeTextFile(t, fileA, "a1\na2\n")
		writeTextFile(t, fileB, "b1\nb2\n")

		source := NewFileLineStream([]string{fileA, fileB})
		got := source.Collect()

		want := []string{"a1", "a2", "b1", "b2"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Collect() = %v, want %v", got, want)
		}
		if err := source.Err(); err != nil {
			t.Fatalf("Err() = %v, want nil", err)
		}
	})

	t.Run("works with the transform pipeline", func(t *testing.T) {
		dir := t.TempDir()
		fileA := filepath.Join(dir, "events-1.log")
		fileB := filepath.Join(dir, "events-2.log")

		writeTextFile(t, fileA, "apple\nbanana\napple\n")
		writeTextFile(t, fileB, "orange\nbanana\napple\n")

		source := NewFileLineStream([]string{fileA, fileB})
		count := source.Filter(func(v string) bool { return v == "apple" }).Count()

		if count != 3 {
			t.Fatalf("apple count = %d, want 3", count)
		}
		if err := source.Err(); err != nil {
			t.Fatalf("Err() = %v, want nil", err)
		}
	})

	t.Run("Err propagates through derived streams", func(t *testing.T) {
		dir := t.TempDir()
		fileA := filepath.Join(dir, "a.txt")
		missing := filepath.Join(dir, "missing.txt")
		writeTextFile(t, fileA, "a1\n")

		derived := NewFileLineStream([]string{fileA, missing}).
			Map(func(s string) string { return strings.ToUpper(s) })

		got := derived.Collect()
		if !reflect.DeepEqual(got, []string{"A1"}) {
			t.Fatalf("Collect() = %v, want [A1]", got)
		}
		if err := derived.Err(); err == nil {
			t.Fatal("Err() = nil, want non-nil")
		}
	})

	t.Run("stops with error when file does not exist", func(t *testing.T) {
		dir := t.TempDir()
		fileA := filepath.Join(dir, "a.txt")
		missing := filepath.Join(dir, "missing.txt")

		writeTextFile(t, fileA, "a1\n")

		source := NewFileLineStream([]string{fileA, missing})
		got := source.Collect()

		want := []string{"a1"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Collect() = %v, want %v", got, want)
		}
		if err := source.Err(); err == nil {
			t.Fatal("Err() = nil, want non-nil")
		}
	})

	t.Run("resets Err per iteration run", func(t *testing.T) {
		dir := t.TempDir()
		fileA := filepath.Join(dir, "a.txt")
		missing := filepath.Join(dir, "missing.txt")
		writeTextFile(t, fileA, "a1\na2\n")

		source := NewFileLineStream([]string{fileA, missing})

		_ = source.Collect()
		if err := source.Err(); err == nil {
			t.Fatal("first run Err() = nil, want non-nil")
		}

		value, ok := source.First()
		if !ok || value != "a1" {
			t.Fatalf("First() = (%q, %v), want (\"a1\", true)", value, ok)
		}
		if err := source.Err(); err != nil {
			t.Fatalf("second run Err() = %v, want nil", err)
		}
	})

	t.Run("reads final line without trailing newline", func(t *testing.T) {
		dir := t.TempDir()
		fileA := filepath.Join(dir, "a.txt")
		writeTextFile(t, fileA, "a1\na2")

		source := NewFileLineStream([]string{fileA})
		got := source.Collect()
		want := []string{"a1", "a2"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Collect() = %v, want %v", got, want)
		}
		if err := source.Err(); err != nil {
			t.Fatalf("Err() = %v, want nil", err)
		}
	})
}

func writeTextFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write %s: %v", path, err)
	}
}

func TestNewFileCSVStream(t *testing.T) {
	t.Run("reads CSV records across multiple files", func(t *testing.T) {
		dir := t.TempDir()
		fileA := filepath.Join(dir, "a.csv")
		fileB := filepath.Join(dir, "b.csv")

		writeTextFile(t, fileA, "apple,2\nbanana,1\n")
		writeTextFile(t, fileB, "orange,3\n")

		source := NewFileCSVStream([]string{fileA, fileB})
		got := source.Collect()

		want := [][]string{
			{"apple", "2"},
			{"banana", "1"},
			{"orange", "3"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Collect() = %v, want %v", got, want)
		}
		if err := source.Err(); err != nil {
			t.Fatalf("Err() = %v, want nil", err)
		}
	})

	t.Run("works with the transform pipeline", func(t *testing.T) {
		dir := t.TempDir()
		fileA := filepath.Join(dir, "events-1.csv")
		fileB := filepath.Join(dir, "events-2.csv")

		writeTextFile(t, fileA, "apple,ok\nbanana,ok\n")
		writeTextFile(t, fileB, "apple,ng\napple,ok\n")

		source := NewFileCSVStream([]string{fileA, fileB})
		count := source.
			Filter(func(row []string) bool { return len(row) > 0 && row[0] == "apple" }).
			Count()

		if count != 3 {
			t.Fatalf("apple count = %d, want 3", count)
		}
		if err := source.Err(); err != nil {
			t.Fatalf("Err() = %v, want nil", err)
		}
	})

	t.Run("returns parse error with malformed CSV", func(t *testing.T) {
		dir := t.TempDir()
		fileA := filepath.Join(dir, "ok.csv")
		fileB := filepath.Join(dir, "broken.csv")

		writeTextFile(t, fileA, "a,1\n")
		writeTextFile(t, fileB, "\"unclosed,2\n")

		source := NewFileCSVStream([]string{fileA, fileB})
		got := source.Collect()

		want := [][]string{{"a", "1"}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Collect() = %v, want %v", got, want)
		}
		if err := source.Err(); err == nil {
			t.Fatal("Err() = nil, want non-nil")
		}
	})
}

type splitParser struct {
	sep string
}

func (p splitParser) Parse(_ string, r io.Reader, yield func([]string) bool) error {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if !yield(strings.Split(line, p.sep)) {
			return nil
		}
	}
	return scanner.Err()
}

func TestParseFilesWithCustomParser(t *testing.T) {
	dir := t.TempDir()
	fileA := filepath.Join(dir, "a.pipe")
	fileB := filepath.Join(dir, "b.pipe")

	writeTextFile(t, fileA, "k1|v1\nk2|v2\n")
	writeTextFile(t, fileB, "k3|v3\n")

	source := ParseFiles[[]string](NewFileStream([]string{fileA, fileB}), splitParser{sep: "|"})
	got := source.Collect()

	want := [][]string{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Collect() = %v, want %v", got, want)
	}
	if err := source.Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
}
