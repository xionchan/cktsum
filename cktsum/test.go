package main

import (
	"os"
	"sync"
	"time"
)

func main() {
	var sourcewg sync.WaitGroup
	sourcewg.Add(1)
	go func() {
		defer sourcewg.Done()
		s := 1
		for {
			s = s + 1
			time.Sleep(1500 * time.Millisecond)
			print("cxw\n")
			if s > 100000 {
				break
			}
		}

	}()

	go func() {
		time.Sleep(4500 * time.Millisecond)
		os.Exit(0)
	}()

	sourcewg.Wait()
}
