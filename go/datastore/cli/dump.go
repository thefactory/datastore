package main

import (
	"fmt"
	"github.com/thefactory.com/datastore/go/datastore"
	"log"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: dump <tablet>")
	}
	tablet := os.Args[1]

	t, err := datastore.OpenTabletFile(tablet)
	if err != nil {
		log.Print(err)
	}

	iter := t.Find(nil)
	for iter.Next() {
		fmt.Printf("%s: %s\n", iter.Key(), iter.Value())
	}
}
