package main

import (
	"context"
	"fmt"
)

func main() {
	//x()
	fmt.Println(int(^uint(0) >> 1))
	fmt.Println(int64(^uint64(0) >> 1))
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
