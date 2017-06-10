package goavro_test

import (
	"runtime"
	"strings"
	"sync"
	"testing"
)

func benchmarkLowAndHigh(b *testing.B, callback func()) {
	// Run test case in parallel at relative low concurrency
	b.Run("Low", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				callback()
			}
		})
	})

	// Run test case in parallel at relative high concurrency
	b.Run("High", func(b *testing.B) {
		concurrency := runtime.NumCPU() * 1000
		wg := new(sync.WaitGroup)
		wg.Add(concurrency)
		b.ResetTimer()

		for c := 0; c < concurrency; c++ {
			go func() {
				defer wg.Done()

				for n := 0; n < b.N; n++ {
					callback()
				}
			}()
		}

		wg.Wait()
	})
}

// ensure code under test returns error containing specified string
func ensureError(tb testing.TB, err error, contains ...string) {
	if err == nil {
		tb.Errorf("Actual: %v; Expected: %#v", err, contains)
		return
	}
	for _, stub := range contains {
		if !strings.Contains(err.Error(), stub) {
			tb.Errorf("Actual: %v; Expected: %#v", err, contains)
		}
	}
}
