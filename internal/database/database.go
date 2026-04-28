package database

import (
	"log"

	"github.com/Achno2k/go-task-queue/internal/models"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var DB *gorm.DB

func ConnectDB(dsn string) {
	var err error

	DB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})

	if err != nil {
		log.Fatal("Failed to connect to DB: ", err)
	}

	DB.AutoMigrate(&models.Task{})

	log.Println("Connected to DB")
}
