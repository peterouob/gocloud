package db

import (
	"errors"
	"log"
	"os"
)

func Start() {
	initDb()
}

func initDb() {
	if _, err := os.OpenFile("./d", os.O_CREATE|os.O_RDWR, 0777); err != nil {
		log.Println(errors.New("error in Open file" + err.Error()))
	}
}
