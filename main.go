package main

import (
	"log"

	"github.com/tass-io/scheduler/cmd"
)

func main() {
	err := cmd.Execute()
	if err != nil {
		log.Fatalf("cmd.Execute error: %v", err)
	}
}
