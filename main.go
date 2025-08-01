package main

import (
	"os"

	"github.com/trade-back/internal/commands"
)

func main() {
	if err := commands.Execute(); err != nil {
		os.Exit(1)
	}
}