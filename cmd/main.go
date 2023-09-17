package main

import (
	"log"
	"message-relay/internal/app"
	"os"
)

func main() {
	if err := app.Run(); err != nil {
		log.Fatal(err.Error())
		os.Exit(1)
	}
}
