package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	PostgresDSN		string
	HTTPPort		int
	KafkaBrokers	[]string
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	// os.Getenv returns "" if the variable isn't set — that's why every field has a fallback default, so the app still runs with zero .env setup.

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/jobscheduler"
	}

	port := 8080
	if p := os.Getenv("HTTP_PORT"); p != "" {
		v, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("invalid HTTP_PORT: %w", err)
		}
		port = v
	}

	brokers := []string{"localhost:9092"}
	if b := os.Getenv("KAFKA_BROKERS"); b != ""{
		brokers = strings.Split(b,",")
	}


	return &Config{
		PostgresDSN: dsn,
		HTTPPort: port,
		KafkaBrokers: brokers,
	}, nil
}