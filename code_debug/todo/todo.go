package main

import (
	"fmt"
	"os/exec"
)

func main() {
	x()
}

func x() {
	out, _ := exec.Command( "zsh","-c","which kill-port.sh").CombinedOutput()
	fmt.Println(string(out))
}
