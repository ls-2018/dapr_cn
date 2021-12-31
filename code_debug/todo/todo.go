package main

import (
	"context"
	"fmt"
	"sort"
)

func main() {
	//x()
	fmt.Println(int(^uint(0) >> 1))
	fmt.Println(int64(^uint64(0) >> 1))
	a:=[]int{1,2,3,4,5,6,7}
	x:=sort.Search(len(a), func(i int) bool {
		return a[i]>3
	})
	fmt.Println(x)

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
