package converter

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"imersaofc/internal/rabbitmq"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

type VideoConverter struct {
	db             *sql.DB
	rabbitmqClient *rabbitmq.RabbitClient
}

func NewVideoConverter(rabbitmqClient *rabbitmq.RabbitClient, db *sql.DB) *VideoConverter {
	return &VideoConverter{
		rabbitmqClient: rabbitmqClient,
		db:             db,
	}
}

type VideoTask struct {
	VideoID int    `json:"video_id"`
	Path    string `json:"path"`
}

func (vc *VideoConverter) Handle(d amqp.Delivery, conversionExch, comfirmationKey, confirmationQueue string) {
	var task VideoTask
	err := json.Unmarshal(d.Body, &task)
	if err != nil {
		vc.logError(task, "Failed to unmarshal task", err)
		return
	}

	if IsProcessed(vc.db, task.VideoID) {
		slog.Warn("Video already processed", slog.Int("video_id", task.VideoID))
		d.Ack(false)
		return
	}

	err = vc.processVideo(&task)
	if err != nil {
		vc.logError(task, "Failed to process video", err)
		return
	}

	err = MarkProcessed(vc.db, task.VideoID)
	if err != nil {
		vc.logError(task, "Failed to mark video as processed", err)
		return
	}
	d.Ack(false)
	slog.Info("Video marked as processed", slog.Int("video_id", task.VideoID))

	confirmationMessage := []byte(fmt.Sprintf(`{"video_id": %d, "path": "%s"}`, task.VideoID, task.Path))
	err = vc.rabbitmqClient.PublishMessage(conversionExch, comfirmationKey, confirmationQueue, confirmationMessage)

}

func (vc *VideoConverter) processVideo(task *VideoTask) error {
	mergedFile := filepath.Join(task.Path, "merged.mp4")
	mpegDashPath := filepath.Join(task.Path, "mpeg-dash")

	slog.Info("Merging chunks", slog.String("path", task.Path))
	err := vc.mergeChunks(task.Path, mergedFile)
	if err != nil {
		vc.logError(*task, "Failed to merge chunks", err)
		return err
	}
	slog.Info("Creating mpeg-dash dir", slog.String("path", task.Path))
	err = os.MkdirAll(mpegDashPath, os.ModeAppend)
	if err != nil {
		vc.logError(*task, "Failed to create mpeg-dash directory", err)
		return err
	}
	slog.Info("Converting to mpeg-dash", slog.String("path", task.Path))
	ffmpegCmd := exec.Command(
		"ffmpeg", "-i", mergedFile,
		"-f", "dash",
		filepath.Join(mpegDashPath, "output.mpd"),
	)

	output, err := ffmpegCmd.CombinedOutput()
	if err != nil {
		vc.logError(*task, "Failed to convert to mpeg-dash, output"+string(output), err)
		return err
	}
	slog.Info("Video converted to mpeg-dash", slog.String("path", mpegDashPath))
	err = os.Remove(mergedFile)
	if err != nil {
		vc.logError(*task, "Failed to remove merged file", err)
		return err
	}
	return nil
}

func (vc *VideoConverter) logError(task VideoTask, message string, err error) {
	errorData := map[string]any{
		"video_id": task.VideoID,
		"error":    message,
		"details":  err.Error(),
		"time":     time.Now(),
	}
	serializedError, _ := json.Marshal(errorData)
	slog.Error("Processing error", slog.String("error_details", string(serializedError)))

	RegisterError(vc.db, errorData, err)

}

func (vc *VideoConverter) extractNumber(fileName string) int {
	re := regexp.MustCompile(`(\d+)`)
	numStr := re.FindString(filepath.Base(fileName))
	num, err := strconv.Atoi(numStr)
	if err != nil {
		return -1
	}
	return num
}

func (vc *VideoConverter) mergeChunks(inputDir string, outputFile string) error {
	// Get all chunk files in the input directory
	chunks, err := filepath.Glob(filepath.Join(inputDir, "*.chunk"))
	if err != nil {
		return fmt.Errorf("failed to find chunks: %v", err)
	}
	sort.Slice(chunks, func(i, j int) bool {
		return vc.extractNumber(chunks[i]) < vc.extractNumber(chunks[j])
	})
	output, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer output.Close()
	for _, chunk := range chunks {
		input, err := os.Open(chunk)
		if err != nil {
			return fmt.Errorf("failed to read chunk file: %v", err)
		}
		_, err = output.ReadFrom(input)
		if err != nil {
			return fmt.Errorf("failed to write chunk %s to merged file: %v", chunk, err)
		}
		input.Close()
	}
	return nil
}
