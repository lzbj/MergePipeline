package main

import (
	"github.com/lzbj/pipeline/pipe"
	"fmt"
	"os"
	"bufio"
)

const filename = "small.dat"

const num = 800000

func main() {

	file, err := os.Create(filename)

	if err != nil {
		panic(err)
	}

	defer file.Close()

	p := pipe.RandomSrc(num)
	wter := bufio.NewWriter(file)

	pipe.WriterOut(wter, p)
	wter.Flush()

	file, err = os.Open(filename)

	if err != nil {
		panic(err)
	}

	defer file.Close()
	p = pipe.ReaderSource(bufio.NewReader(file), -1)
	count :=0
	for v := range p {

		fmt.Println(v)
		count++
		if count>=200{
			break
		}
	}

}

func MemoMerge() {
	p := pipe.Merge(
		pipe.MemSort(pipe.ArraySource(32423, 32, 233, 23, 1, 434)),
		pipe.MemSort(pipe.ArraySource(2, 13, 34, 243, 55, 5667)))
	for v := range p {
		fmt.Printf("%d\n", v)
	}

}
