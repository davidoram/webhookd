package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"
)

type Payload struct {
	Data int `json:"data"`
}

func main() {
	// Read csvFile, and rows, and duration from command line arguments
	csvFile := flag.String("csv", "input.csv", "Path to the CSV file, defaults to 'input.csv'")
	rows := flag.Int("rows", 1000, "Number of rows to generate, defaults to 1000")
	topics := flag.Int("topics", 3, "Number of topics to use, defaults to 3")
	dur := flag.Duration("duration", time.Minute*1, "Duration to generate rows for, defaults to 1 min")

	flag.Parse()

	file, err := os.Create(*csvFile)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	writer.Write([]string{"topic", "payload", "key", "publish_offset_millis"})

	offset := int64(0)
	increment := dur.Milliseconds() / int64(*rows)
	for i := 0; i < *rows; i++ {
		topic := fmt.Sprintf("topic.%d", rand.Intn(*topics)+1)
		key := fmt.Sprintf("%d", i+1)

		// Generate a random payload
		payload := &Payload{Data: rand.Intn(100)}
		payloadBytes, _ := json.Marshal(payload)
		payloadStr := string(payloadBytes)

		publish_offset_millis := fmt.Sprintf("%d", offset)
		offset = offset + increment

		writer.Write([]string{topic, payloadStr, key, publish_offset_millis})
	}
}
