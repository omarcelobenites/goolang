package main

import (
	"database/sql"
	"fmt"
	"imersaofc/internal/converter"
	"imersaofc/internal/rabbitmq"
	"log/slog"
	"os"

	_ "github.com/lib/pq"
	"github.com/streadway/amqp"
)

func connectPostgres() (*sql.DB, error) {
	user := getEnvOrDefault("POSTGRES_USER", "user")
	password := getEnvOrDefault("POSTGRES_PASSWORD", "password")
	dbname := getEnvOrDefault("POSTGRES_DB", "converter")
	host := getEnvOrDefault("POSTGRES_HOST", "postgres")
	sslmode := getEnvOrDefault("POSTGRES_SSLMODE", "disable")

	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s sslmode=%s", user, password, dbname, host, sslmode)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		slog.Error("Error connecting to database", slog.String("connStr", connStr))
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		slog.Error("Error pinging database", slog.String("connStr", connStr))
		return nil, err
	}
	slog.Info("Connected to Postgres successfully")

	return db, nil
}

func getEnvOrDefault(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func main() {
	db, err := connectPostgres()
	if err != nil {
		panic(err)
	}
	rabbitMQURL := getEnvOrDefault("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
	rabbitClient, err := rabbitmq.NewRabbitClient(rabbitMQURL)
	if err != nil {
		panic(err)
	}

	defer rabbitClient.Close()

	conversionExch := getEnvOrDefault("CONVERSION_EXCHANGE", "conversion_exchange")
	queueName := getEnvOrDefault("CONVERSION_QUEUE", "video_conversion_queue")
	conversionKey := getEnvOrDefault("CONVERSION_KEY", "conversion")
	confirmationKey := getEnvOrDefault("CONFIRMATION_KEY", "finish-conversion")
	confirmationQueue := getEnvOrDefault("CONFIRMATION_QUEUE", "video_confirmation_queue")

	vc := converter.NewVideoConverter(rabbitClient, db)
	// vc.Handle([]byte(`{"video_id": 1, "path": "/media/uploads/1"}`))

	msgs, err := rabbitClient.ConsumeMessages(conversionExch, conversionKey, queueName)
	if err != nil {
		slog.Error("failed to consume messages", slog.String("error", err.Error()))
	}

	for d := range msgs {
		go func(delivery amqp.Delivery) {
			vc.Handle(delivery, conversionExch, confirmationKey, confirmationQueue)
		}(d)
	}

}
