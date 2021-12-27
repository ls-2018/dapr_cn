package main

import (
	"context"
	"fmt"
)

func main() {
	x()
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
