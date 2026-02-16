package main

import (
	"os"
	"path/filepath"
	"reflect"
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
		got := Stream(source.Seq, End(Collect[string]()))

		want := []string{"a1", "a2", "b1", "b2"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Stream() = %v, want %v", got, want)
		}
		if err := source.Err(); err != nil {
			t.Fatalf("Err() = %v, want nil", err)
		}
	})

	t.Run("works with existing filters and aggregate", func(t *testing.T) {
		dir := t.TempDir()
		fileA := filepath.Join(dir, "events-1.log")
		fileB := filepath.Join(dir, "events-2.log")

		writeTextFile(t, fileA, "apple\nbanana\napple\n")
		writeTextFile(t, fileB, "orange\nbanana\napple\n")

		source := NewFileLineStream([]string{fileA, fileB})
		count := Stream(
			source.Seq,
			Filter(func(v string) bool { return v == "apple" },
				End(Count[string]()),
			),
		)

		if count != 3 {
			t.Fatalf("apple count = %d, want 3", count)
		}
		if err := source.Err(); err != nil {
			t.Fatalf("Err() = %v, want nil", err)
		}
	})

	t.Run("stops with error when file does not exist", func(t *testing.T) {
		dir := t.TempDir()
		fileA := filepath.Join(dir, "a.txt")
		missing := filepath.Join(dir, "missing.txt")

		writeTextFile(t, fileA, "a1\n")

		source := NewFileLineStream([]string{fileA, missing})
		got := Stream(source.Seq, End(Collect[string]()))

		want := []string{"a1"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Stream() = %v, want %v", got, want)
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

		_ = Stream(source.Seq, End(Collect[string]()))
		if err := source.Err(); err == nil {
			t.Fatal("first run Err() = nil, want non-nil")
		}

		first := Stream(source.Seq, End(First[string]()))
		if !first.OK || first.Value != "a1" {
			t.Fatalf("First() = (%q, %v), want (\"a1\", true)", first.Value, first.OK)
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
		got := Stream(source.Seq, End(Collect[string]()))
		want := []string{"a1", "a2"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Stream() = %v, want %v", got, want)
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
