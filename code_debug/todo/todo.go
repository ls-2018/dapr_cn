package main

import (
	"fmt"
	"os"
)

func main() {
	os.Setenv("A","123")
	b:=[]byte(`"${A}"XZXC`)
	b = []byte(os.ExpandEnv(string(b)))
	fmt.Println(string(b),"---")
}
