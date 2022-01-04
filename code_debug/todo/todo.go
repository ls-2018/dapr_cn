package main

import (
	"context"
	"fmt"
	"github.com/dapr/dapr/pkg/actors"
	"sort"
	"time"
)

func main() {
	//x()
	fmt.Println(int(^uint(0) >> 1))
	fmt.Println(int64(^uint64(0) >> 1))
	a := []int{1, 2, 3, 4, 5, 6, 7}
	x := sort.Search(len(a), func(i int) bool {
		return a[i] > 3
	})
	fmt.Println(x)
	//b, _ := actors.GetParseTime("0s", nil)
	d, _ := actors.GetParseTime("-0h30m0s", nil)
	fmt.Println(d.Before(time.Now()))
	fmt.Println(time.Now())
	fmt.Println("----")
	//c := time.Now().Add(time.Second)

	//fmt.Println(time.Now().After(time.Now()))
	//c := time.NewTimer(-time.Second * 10)
	//for i := range c.C {
	//	fmt.Println(i)
	//}

	var de map[string][]string
	for item := range de["a"]{
		fmt.Println(item)
	}

}

func x() {
	ctx, _ := context.WithCancel(context.Background())
	queue := make(chan int)
	select {
	// 处理取消的问题
	case <-ctx.Done():
		return

	case msg := <-queue:
		fmt.Println(msg)
	}
	fmt.Println(123)
}
