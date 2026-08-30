package main

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func writeBenchFile(b *testing.B, path string, lines []string) {
	b.Helper()

	content := strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		b.Fatalf("WriteFile(%q) returned error: %v", path, err)
	}
}

func benchLineFile(b *testing.B, lineCount int) string {
	b.Helper()

	path := filepath.Join(b.TempDir(), "lines.log")
	writeBenchFile(b, path, benchKeys("line-", lineCount))
	return path
}

func benchCSVFile(b *testing.B, recordCount int) string {
	b.Helper()

	records := make([]string, recordCount)
	for i := range records {
		id := strconv.Itoa(i)
		records[i] = id + ",name-" + id + ",\"quoted, value " + id + "\""
	}

	path := filepath.Join(b.TempDir(), "records.csv")
	writeBenchFile(b, path, records)
	return path
}

func BenchmarkNewFileLineStream(b *testing.B) {
	for _, n := range benchSizes {
		path := benchLineFile(b, n)
		b.Run(sizeLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				stream := NewFileLineStream([]string{path})
				count := stream.Count()
				if err := stream.Err(); err != nil {
					b.Fatalf("Err() returned error: %v", err)
				}
				if count != n {
					b.Fatalf("Count() = %d, expected %d", count, n)
				}
			}
		})
	}
}

func BenchmarkFileLineStreamBaselineBufio(b *testing.B) {
	for _, n := range benchSizes {
		path := benchLineFile(b, n)
		b.Run(sizeLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				file, err := os.Open(path)
				if err != nil {
					b.Fatalf("Open(%q) returned error: %v", path, err)
				}

				count := 0
				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					count++
				}
				if err := scanner.Err(); err != nil {
					b.Fatalf("Scan() returned error: %v", err)
				}
				if err := file.Close(); err != nil {
					b.Fatalf("Close() returned error: %v", err)
				}
				if count != n {
					b.Fatalf("scanned %d lines, expected %d", count, n)
				}
			}
		})
	}
}

func BenchmarkFileLineStreamPipeline(b *testing.B) {
	for _, n := range benchSizes {
		path := benchLineFile(b, n)
		b.Run(sizeLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				stream := NewFileLineStream([]string{path})
				stream.
					Filter(func(line string) bool { return !strings.HasSuffix(line, "0") }).
					Map(strings.ToUpper).
					GroupBy(func(line string) int { return len(line) })
				if err := stream.Err(); err != nil {
					b.Fatalf("Err() returned error: %v", err)
				}
			}
		})
	}
}

func BenchmarkFileLineStreamTakeEarlyStop(b *testing.B) {
	path := benchLineFile(b, 100000)

	b.ReportAllocs()
	for b.Loop() {
		stream := NewFileLineStream([]string{path})
		result := stream.Take(10).Collect()
		if len(result) != 10 {
			b.Fatalf("Take(10) yielded %d lines", len(result))
		}
	}
}

func BenchmarkNewFileCSVStream(b *testing.B) {
	for _, n := range benchSizes {
		path := benchCSVFile(b, n)
		b.Run(sizeLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				stream := NewFileCSVStream([]string{path})
				count := stream.Count()
				if err := stream.Err(); err != nil {
					b.Fatalf("Err() returned error: %v", err)
				}
				if count != n {
					b.Fatalf("Count() = %d, expected %d", count, n)
				}
			}
		})
	}
}

func BenchmarkFileStreamManyFiles(b *testing.B) {
	const fileCount = 64
	dir := b.TempDir()
	paths := make([]string, fileCount)
	for i := range paths {
		paths[i] = filepath.Join(dir, "part-"+strconv.Itoa(i)+".log")
		writeBenchFile(b, paths[i], benchKeys("line-", 100))
	}

	b.ReportAllocs()
	for b.Loop() {
		stream := NewFileLineStream(paths)
		count := stream.Count()
		if err := stream.Err(); err != nil {
			b.Fatalf("Err() returned error: %v", err)
		}
		if count != fileCount*100 {
			b.Fatalf("Count() = %d, expected %d", count, fileCount*100)
		}
	}
}
