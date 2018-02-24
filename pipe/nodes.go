package pipe

import (
	"sort"
	"io"
	"encoding/binary"
	"math/rand"
	"time"
	"fmt"
)

var startTime time.Time

func Init(){
	startTime = time.Now()
}
func ArraySource(a ... int) <-chan int {
	out := make(chan int)

	go func() {
		for _, v := range a {
			out <- v
		}
		close(out)
	}()
	return out
}

func MemSort(in <-chan int) <-chan int {
	out := make(chan int,1024)

	go func() {
		a := [] int{}
		for v := range in {
			a = append(a, v)
		}
        fmt.Println("Read done:",time.Now().Sub(startTime))
		sort.Ints(a)
		fmt.Println("InMemo Sort done:",time.Now().Sub(startTime))
		for _, v := range a {
			out <- v
		}
		close(out)
	}()
	return out
}

func Merge(in1, in2 <-chan int) <-chan int {
	out := make(chan int,1024)
	go func() {
		v1, o1 := <-in1
		v2, o2 := <-in2

		for o1 || o2 {
			if !o2 || (o1 && v1 <= v2) {
				out <- v1
				v1, o1 = <-in1
			} else {
				out <- v2
				v2, o2 = <-in2
			}
		}
		close(out)
		fmt.Println("Merge Done:", time.Now().Sub(startTime))
	}()

	return out
}

func ReaderSource(reader io.Reader, chunkSize int) <-chan int {
	out := make(chan int,1024)
	go func() {

		bfr := make([]byte, 8)
		bytesRead := 0
		for {
			n, err := reader.Read(bfr)
			bytesRead+=n
			if n > 0 {
				v := int(binary.BigEndian.Uint64(bfr))
				out <- v
			}
			if err != nil || ((chunkSize != -1 && bytesRead >= chunkSize)) {
				break
			}
		}
		close(out)
	}()
	return out
}

func WriterOut(writer io.Writer, in <-chan int) {
	for v := range in {
		buffer := make([]byte, 8)
		binary.BigEndian.PutUint64(buffer, uint64(v))
		writer.Write(buffer)
	}
}

func RandomSrc(count int) <-chan int {
	out := make(chan int)

	go func() {
		for i := 0; i < count; i++ {
			out <- rand.Int()
		}
		close(out)
	}()

	return out
}

func MergeN(inputs ...<-chan int) <-chan int {
	if len(inputs) == 1 {
		return inputs[0]
	}

	m := len(inputs) / 2
	return Merge(MergeN(inputs[:m]...),
		MergeN(inputs[m:]...))

}
