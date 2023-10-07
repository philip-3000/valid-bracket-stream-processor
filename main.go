package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

/*
Writes [{....}] to the channel, where the number of repeated terms is
specifed by the count parameter.
*/
func generateValidStream(stream chan<- byte, count int, wg *sync.WaitGroup) int64 {
	var totalSize int64 = 0
	for i := 0; i < count; i++ {
		wg.Add(3)
		stream <- '['
		stream <- '{'
		stream <- 'a'
		totalSize += 3
	}

	for i := 0; i < count; i++ {
		wg.Add(2)
		stream <- '}'
		stream <- ']'
		totalSize += 2
	}
	return totalSize
}

/*
Writes 'size' - 5 open brackets to the stream, but, 'size' closing brackets.
*/
func generateInsufficientOpenBrackets(stream chan<- byte, size int, wg *sync.WaitGroup) int64 {
	var totalSize int64 = 0
	for i := 0; i < size-5; i++ {
		wg.Add(1)
		stream <- '['

		totalSize += 1
	}

	for i := 0; i < size; i++ {
		wg.Add(1)
		stream <- ']'
		totalSize += 1
	}

	return totalSize
}

func main() {
	fmt.Println("Running a few tests:")
	fmt.Println("\t * Generate a small valid bracket stream")
	fmt.Println("\t * Generate a valid bracket stream of > 1MB of characters")
	fmt.Println("\t * Generate an invalid bracket stream of not enough open brackets")
	fmt.Println("\t * Generate another small valid bracket stream")
	fmt.Println("\t * Generate a larger valid bracket stream over 256MB of characters")

	// control - c to exit.
	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	fmt.Printf("\nPress ctrl+c to continue...\n")

	// use a file as our stream/buffer as we should be able to
	// append to the stream but also read from it.
	writeFile, err := os.Create("buffer.txt")
	if err != nil {
		fmt.Printf("error occured: %s", err.Error())
		os.Exit(-1)
	}
	defer writeFile.Close()

	// Let's utilize a channel to move the data to our processing go routine.
	// we'll collect 64K at a time.
	var bracketStream = make(chan byte, 64*1024)

	// for synchronizing tests..
	var wg sync.WaitGroup
	go func() {
		// character buffer to read into the (file) stream
		// note: reading and writing character by character isn't efficient at all - but I think
		// it illustrates the concept. To make this more efficient, we should utilize a block/buffer/buffered io
		// and buffer the reads/writes in blocks.
		readBuffer := make([]byte, 1)

		// map to map our opening brace to closing brace
		var openToCloseMap = map[byte]byte{'{': '}', '[': ']', '(': ')'}
		var closeToOpenMap = map[byte]byte{'}': '{', ']': '[', ')': '('}

		// byte offset to keep track
		var offset int64 = 0
		var totalSize int64 = 0
		for {
			select {
			case character, ok := <-bracketStream:
				if ok {
					// check the character
					matchingOpenBracket, foundClosingBracket := closeToOpenMap[character]
					_, foundOpenBracket := openToCloseMap[character]

					// nothing to do if it's not an open or close bracket.
					if !(foundClosingBracket || foundOpenBracket) {
						wg.Done()
						continue
					}

					if foundOpenBracket {
						// write out open brackets.
						_, err := writeFile.WriteAt([]byte{character}, offset)
						if err != nil {
							fmt.Printf("Error occurred: %s", err.Error())
							os.Exit(-1)
						}

						// bump our offset
						offset += 1
						totalSize += 1
						wg.Done()
						continue
					}

					// at this point must be a closing bracket, we need to check the one that
					// came before it matches up.
					totalSize += 1

					// check the offset...
					if offset < 1 {
						fmt.Printf("*****Invalid Stream: Exhausted offset - cannot read anymore open brackets from stream.\n")
						totalSize = 0
						wg.Done()
						continue
					}

					// offset ok.  read the last bracket.
					offset -= 1
					writeFile.ReadAt(readBuffer, offset)

					// don't quite have to check this since it's all we were putting on for
					// our example...
					if readBuffer[0] != matchingOpenBracket {
						fmt.Printf("*****Found invalid character '%c' but expected '%c' - stream not valid so far. Resetting offset.\n", readBuffer[0], matchingOpenBracket)
						offset = 0
						totalSize = 0
						wg.Done()
						continue
					}

					// ok, matching open bracket. check offset. if it's at 0, we've successfully found a valid stream
					if offset == 0 {
						fmt.Printf("*****Found a valid bracket stream! Total bracket count = %d\n", totalSize)

						totalSize = 0
						wg.Done()
						continue
					}
					wg.Done()
				}

			}
		}
	}()

	go func() {
		// could put some synchronization in here...
		fmt.Printf("First test...\n")
		generateValidStream(bracketStream, 1, &wg)
		wg.Wait()
		fmt.Printf("Second test...\n")
		generateValidStream(bracketStream, 512*1024, &wg)
		wg.Wait()
		fmt.Printf("Third test...\n")
		generateInsufficientOpenBrackets(bracketStream, 512, &wg)
		wg.Wait()
		fmt.Printf("Fourth test...\n")
		generateValidStream(bracketStream, 1024, &wg)
		wg.Wait()

		fmt.Printf("Fifth test...\n")
		generateValidStream(bracketStream, 32*1024*1024, &wg)
		wg.Wait()

		fmt.Printf("\nTests Complete.  Control-C to quit.\n")

	}()

	<-done // Will block here until user hits ctrl+c
}
