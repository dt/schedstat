// gentrace generates a test trace file for schedstat.
package main

import (
	"fmt"
	"os"
	"runtime"
	"runtime/trace"
	"sync"
	"time"
)

func main() {
	f, err := os.Create("test.trace")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error creating trace file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	if err := trace.Start(f); err != nil {
		fmt.Fprintf(os.Stderr, "error starting trace: %v\n", err)
		os.Exit(1)
	}

	// Generate some scheduling activity
	var wg sync.WaitGroup
	numGoroutines := runtime.GOMAXPROCS(0) * 4

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Do some work that causes scheduling
			work(id)
		}(i)
	}

	wg.Wait()

	// Force a GC to get GC events
	runtime.GC()

	// More work after GC
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			work(id)
		}(i)
	}
	wg.Wait()

	trace.Stop()
	fmt.Println("Trace written to test.trace")
}

func work(id int) {
	// Simulate CPU-bound work with occasional yields
	sum := 0
	for i := 0; i < 100000; i++ {
		sum += i * id
		if i%10000 == 0 {
			// Allocate to trigger potential GC assist
			_ = make([]byte, 1024)
			time.Sleep(time.Microsecond)
		}
	}
	_ = sum
}
