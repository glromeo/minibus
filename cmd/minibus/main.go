package main

import (
	"log"
)
import . "github.com/glromeo/minibus/internal/minibus"

func main() {
	// Example usage:
	if err := Run(
		WithPort("4060"),
		WithBufferSize(2048),
	); err != nil {
		log.Fatal(err)
	}
}
