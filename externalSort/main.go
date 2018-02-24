package main

import (
	"os"
	"github.com/lzbj/pipeline/pipe"
	"bufio"
	"fmt"
)

func main() {
	p := createPipeline("../small.dat", 6400000, 8)
	writeToFile(p, "small.dat")
	printFile("small.dat")

}

func createPipeline(filename string, fileSize, chunkCount int) <-chan int {
	pipe.Init()
	chunksize := fileSize / chunkCount

	sortResults := []<-chan int{}

	for i := 0; i < chunkCount; i++ {
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}

		file.Seek(int64(i*chunksize), 0)

		source := pipe.ReaderSource(bufio.NewReader(file), chunksize)
		sortResults = append(sortResults, pipe.MemSort(source))

	}

	return pipe.MergeN(sortResults...)

}

func writeToFile(p <-chan int, filename string) {
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}

	defer file.Close()

	wrt := bufio.NewWriter(file)

	defer wrt.Flush()
	pipe.WriterOut(wrt, p)

}

func printFile(filename string) {
	var count =0
	file, err := os.Open(filename)
	if (err != nil) {
		panic(err)
	}

	defer file.Close()
	p := pipe.ReaderSource(file, -1)

	for v := range p {
		fmt.Println(v)
		count++
		if count>=100{
			break
		}
	}
}
