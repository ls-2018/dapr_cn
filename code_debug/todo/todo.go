package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	//x()
	pat, _ := filepath.Abs("~/.dapr/config.yaml")
	fmt.Println(os.Stat(pat))
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
