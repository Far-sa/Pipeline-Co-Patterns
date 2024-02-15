package main

// https://go.dev/blog/pipelines

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

func main() {
	fmt.Println("------------- PIPELINE --------------->")

	//* Stage 1
	in := stage1()

	//* Stage No-2 - multiple writers
	out := make(chan int)

	wg := new(sync.WaitGroup)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go stage2(in, out, wg)
	}

	// none of the multiple writers may close the `out` channel.
	// so another goroutine will watch and close the channel once all the writers are done.
	go func() {
		wg.Wait()
		close(out)
	}()

	//* Stage No-3
	wordCnt := 0
	for wc := range out {
		wordCnt += wc
	}

	fmt.Printf(fmt.Sprintf("number of words are : %d", wordCnt))

}

// stage1() reads all lines of a file and put each line on the channel.
// In other words, stage1() is a generator that returns a receive-only channel
func stage1() <-chan string {

	// to not wait for slow readers we define a buffer for the writer
	in := make(chan string, 5)

	//TODO use ctx
	go func() {

		// To ensure that it closes the channel in either case of success or error
		defer close(in)

		f := readFile()

	LOOP:
		for _, line := range strings.Split(f, "\n") {
			select {
			case in <- line:
				continue
			case <-time.After(time.Millisecond * 10):
				// to not block the writer in case the reader fails to read (crash, etc.)
				// we cancel writing to the channel after a timeout.
				// However, it is an implicit approach. TODO use Context instead
				break LOOP
			}
		}
	}()

	return in
}

func stage2(in <-chan string, out chan<- int, wg *sync.WaitGroup) {
	defer wg.Done()

	for line := range in {
		wordCnt := len(strings.Split(line, " "))
		out <- wordCnt
	}
}

func readFile() string {
	f, err := os.ReadFile("./pipeline/words.txt")
	if err != nil {
		panic(err)
	}
	return string(f)
}
